// Package checkpoint 提供断点的持久化、恢复与合法性校验。
package checkpoint

import (
	"errors"
	"time"
)

// ErrExpired 断点已过期，拒绝恢复。
var ErrExpired = errors.New("checkpoint expired")

// ErrRollback 请求的恢复位点早于已提交位点，属回退，拒绝。
var ErrRollback = errors.New("checkpoint rollback rejected")

// State 是一次断点的完整内容。
type State struct {
	Offset     int64                 // 已提交的源位点（下一条待读）
	Watermark  time.Time             // 已提交的水位线（maxSeen）
	DedupState map[string]DedupEntry // 去重状态快照
	Committed  time.Time             // 断点提交时间
	Generation uint64                // 单调递增的代际号
}

// DedupEntry 是去重状态的可序列化条目。
type DedupEntry struct {
	Fingerprint uint64
	Seq         uint64
}

// Store 是断点的文件存储。
type Store struct {
	path string
	ttl  time.Duration // 断点有效期，超时视为过期
}

// NewStore 创建断点存储；ttl 为断点有效期。
func NewStore(path string, ttl time.Duration) *Store {
	return &Store{path: path, ttl: ttl}
}

// Save 原子地写入断点（先写临时文件再重命名）。
func (s *Store) Save(st State) error {
	return errors.New("not implemented")
}

// Load 读取并校验断点：不存在返回 nil,nil；过期返回 ErrExpired。
func (s *Store) Load() (*State, error) {
	return nil, errors.New("not implemented")
}

// ValidateResume 校验从 resumeOffset 恢复是否合法：
// 早于已提交位点的回退请求返回 ErrRollback。
func ValidateResume(committed, resumeOffset int64) error {
	return nil
}
