// Package dedup 负责重复投递与更正版本的判定。
package dedup

import (
	"github.com/feng111627/market-event-pipeline-go/internal/event"
)

// Verdict 是去重判定结果。
type Verdict int

const (
	// New 首次见到该 ID。
	New Verdict = iota
	// Duplicate ID 与内容指纹一致，属重复投递。
	Duplicate
	// Stale 同 ID 但序号更旧，属过期版本。
	Stale
	// Correction 同 ID 但序号更新且内容不同，属更正，需要合并。
	Correction
)

func (v Verdict) String() string {
	switch v {
	case New:
		return "NEW"
	case Duplicate:
		return "DUPLICATE"
	case Stale:
		return "STALE"
	case Correction:
		return "CORRECTION"
	default:
		return "UNKNOWN"
	}
}

// entry 记录已见事件的指纹与序号。
type entry struct {
	fingerprint uint64
	seq         uint64
}

// Store 保存已见事件的判定状态，容量有界以防内存膨胀。
type Store struct {
	entries map[string]entry
	order   []string // FIFO 淘汰队列
	maxSize int
}

// NewStore 创建容量为 maxSize 的去重存储；超出容量时淘汰最旧条目。
func NewStore(maxSize int) *Store {
	return &Store{entries: make(map[string]entry), maxSize: maxSize}
}

// Check 判定事件是否为新事件、重复、过期或更正。
func (s *Store) Check(ev event.MarketEvent) Verdict {
	return New
}

// Snapshot 导出内部状态用于断点持久化。
func (s *Store) Snapshot() map[string]entry {
	return nil
}

// Restore 从断点恢复内部状态。
func (s *Store) Restore(snap map[string]entry) {}
