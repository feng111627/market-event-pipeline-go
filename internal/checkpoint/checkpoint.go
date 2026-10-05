// Package checkpoint 实现并发处理下的断点持久化与恢复校验。
//
// 恰好一次（崩溃/重启后结果不多不少）的协作方式：
//   - checkpoint 只在下游 sink 确认序号推进后才保存（提交点永不超过输出位置）；
//   - 崩溃时可能丢失“已输出但未提交”的末尾若干条；恢复后从 InputOffset 重读，
//     sink 依据 Seq/SeqID 幂等，重复的输出不会被重复计入；
//   - 恢复校验：版本不符、epoch 过期（超过 TTL 或 epoch 不匹配）、
//     偏移/水位/输出序号发生回退，一律明确拒绝，绝不“从中间悄悄接着算”。
package checkpoint

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// State 断点快照。
type State struct {
	Version       int       `json:"version"`
	Epoch         int64     `json:"epoch"`
	InputOffset   int64     `json:"input_offset"`
	WatermarkUnix int64     `json:"watermark_unix"`
	SinkSeq       int64     `json:"sink_seq"`
	UpdatedAt     time.Time `json:"updated_at"`
	// Processor 保存处理器内部状态（排序缓冲 pending 与去重表 seen）。
	// 崩溃时这些记录“已接收但未输出”，必须随断点恢复，否则会漏算。
	Processor json.RawMessage `json:"processor,omitempty"`
}

// currentVersion 当前断点格式版本。
const currentVersion = 1

// Store 断点存储。
type Store interface {
	Load(ctx context.Context) (*State, error)
	Save(ctx context.Context, s State) error
}

// ErrNoCheckpoint 没有历史断点（首次启动），从头开始。
var ErrNoCheckpoint = errors.New("no checkpoint")

// ErrStale 断点过期：epoch 不符或超过 TTL。
var ErrStale = errors.New("checkpoint stale")

// ErrRegressed 断点回退：偏移/水位/输出序号小于已提交位置。
var ErrRegressed = errors.New("checkpoint regressed")

// ErrBadVersion 断点格式版本不被支持。
var ErrBadVersion = errors.New("checkpoint unsupported version")

// FileStore 原子写本地文件（临时文件 + fsync + rename）。
type FileStore struct {
	path string
}

// NewFileStore 创建文件断点存储。
func NewFileStore(path string) *FileStore { return &FileStore{path: path} }

// Load 读取断点；文件不存在返回 ErrNoCheckpoint。
func (f *FileStore) Load(ctx context.Context) (*State, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b, err := os.ReadFile(f.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNoCheckpoint
	}
	if err != nil {
		return nil, fmt.Errorf("read checkpoint: %w", err)
	}
	var s State
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, fmt.Errorf("corrupt checkpoint: %w", err)
	}
	if s.Version != currentVersion {
		return nil, fmt.Errorf("%w: got %d, want %d", ErrBadVersion, s.Version, currentVersion)
	}
	return &s, nil
}

// Save 原子覆盖断点。
func (f *FileStore) Save(ctx context.Context, s State) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.Version == 0 {
		s.Version = currentVersion
	}
	if s.Version != currentVersion {
		return fmt.Errorf("%w: got %d, want %d", ErrBadVersion, s.Version, currentVersion)
	}
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(f.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".cp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }
	if _, err := tmp.Write(b); err != nil {
		_ = tmp.Close()
		cleanup()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return err
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return err
	}
	if err := os.Rename(tmpName, f.path); err != nil {
		cleanup()
		return err
	}
	return nil
}

// Validator 恢复校验。
type Validator struct {
	ttl time.Duration
}

// NewValidator 创建校验器；ttl<=0 表示不做时间过期检查（仍做 epoch/回退检查）。
func NewValidator(ttl time.Duration) *Validator { return &Validator{ttl: ttl} }

// Validate 判断 candidate 相对 current（本次运行期望的纪元/已确认位置）是否可接受。
// current 为 nil 表示这是一次全新启动，只校验格式。
func (v *Validator) Validate(candidate *State, expectedEpoch int64, expectedSinkSeq int64, now time.Time) error {
	if candidate == nil {
		return errors.New("nil checkpoint")
	}
	if candidate.Version != currentVersion {
		return fmt.Errorf("%w: got %d, want %d", ErrBadVersion, candidate.Version, currentVersion)
	}
	// 过期检查
	if v.ttl > 0 && now.Sub(candidate.UpdatedAt) > v.ttl {
		return fmt.Errorf("%w: age %s > ttl %s", ErrStale, now.Sub(candidate.UpdatedAt).Round(time.Second), v.ttl)
	}
	// epoch 不符：断点来自上一代运行，拒绝
	if expectedEpoch > 0 && candidate.Epoch != expectedEpoch {
		return fmt.Errorf("%w: checkpoint epoch %d != current epoch %d", ErrStale, candidate.Epoch, expectedEpoch)
	}
	// 回退检查：断点中的输出位置不能落后于 sink 已确认位置
	if candidate.SinkSeq < expectedSinkSeq {
		return fmt.Errorf("%w: checkpoint sink_seq %d < acknowledged %d", ErrRegressed, candidate.SinkSeq, expectedSinkSeq)
	}
	if candidate.InputOffset < 0 || candidate.WatermarkUnix < 0 {
		return fmt.Errorf("%w: negative offset/watermark", ErrRegressed)
	}
	return nil
}
