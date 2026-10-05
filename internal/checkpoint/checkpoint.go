// Package checkpoint 提供断点的持久化、恢复与合法性校验。
//
// 语义：
//   - 断点原子写入（临时文件 + rename），崩溃不会留下半截文件。
//   - 断点带有效期（TTL），过期后拒绝恢复（ErrExpired），避免从过旧状态接着算。
//   - 恢复位点早于已提交位点属回退，明确拒绝（ErrRollback）。
package checkpoint

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// ErrExpired 断点已过期，拒绝恢复。
var ErrExpired = errors.New("checkpoint expired")

// ErrRollback 请求的恢复位点早于已提交位点，属回退，拒绝。
var ErrRollback = errors.New("checkpoint rollback rejected")

// State 是一次断点的完整内容。
type State struct {
	Offset     int64                 // 已提交的源位点（下一条待读）
	Watermark  time.Time             // 已提交的水位线基准（maxSeen）
	DedupState map[string]DedupEntry // 去重状态快照
	Committed  time.Time             // 断点提交时间
	Generation uint64                // 单调递增的代际号
}

// DedupEntry 是去重状态的可序列化条目。
type DedupEntry struct {
	Fingerprint uint64 `json:"fingerprint"`
	Seq         uint64 `json:"seq"`
}

// stateFile 是磁盘上的序列化形式。
type stateFile struct {
	Offset     int64                 `json:"offset"`
	Watermark  time.Time             `json:"watermark"`
	DedupState map[string]DedupEntry `json:"dedup_state"`
	Committed  time.Time             `json:"committed"`
	Generation uint64                `json:"generation"`
}

// Store 是断点的文件存储。
type Store struct {
	path string
	ttl  time.Duration
	now  func() time.Time // 可注入时钟，便于测试过期逻辑
}

// NewStore 创建断点存储；ttl 为断点有效期（<=0 表示不过期）。
func NewStore(path string, ttl time.Duration) *Store {
	return &Store{path: path, ttl: ttl, now: time.Now}
}

// SetClock 注入时钟（测试用）。
func (s *Store) SetClock(now func() time.Time) {
	s.now = now
}

// Save 原子地写入断点（先写临时文件再重命名）。
func (s *Store) Save(st State) error {
	if st.Committed.IsZero() {
		st.Committed = s.now()
	}
	f := stateFile{
		Offset:     st.Offset,
		Watermark:  st.Watermark,
		DedupState: st.DedupState,
		Committed:  st.Committed,
		Generation: st.Generation,
	}
	data, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal checkpoint: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return fmt.Errorf("create checkpoint dir: %w", err)
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("write checkpoint tmp: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("commit checkpoint: %w", err)
	}
	return nil
}

// Load 读取并校验断点：不存在返回 nil,nil；过期返回 ErrExpired。
func (s *Store) Load() (*State, error) {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read checkpoint: %w", err)
	}
	var f stateFile
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("corrupt checkpoint: %w", err)
	}
	if s.ttl > 0 && s.now().Sub(f.Committed) > s.ttl {
		return nil, fmt.Errorf("%w: committed %s, ttl %s", ErrExpired, f.Committed, s.ttl)
	}
	return &State{
		Offset:     f.Offset,
		Watermark:  f.Watermark,
		DedupState: f.DedupState,
		Committed:  f.Committed,
		Generation: f.Generation,
	}, nil
}

// ValidateResume 校验从 resumeOffset 恢复是否合法：
// 早于已提交位点的回退请求返回 ErrRollback。
func ValidateResume(committed, resumeOffset int64) error {
	if resumeOffset < committed {
		return fmt.Errorf("%w: resume offset %d < committed %d", ErrRollback, resumeOffset, committed)
	}
	return nil
}
