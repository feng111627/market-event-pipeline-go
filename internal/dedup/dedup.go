// Package dedup 负责重复投递与更正版本的判定。
//
// 判定规则（以事件 ID 为键）：
//   - 首次见到：New。
//   - ID 相同且内容指纹一致：Duplicate（重复投递，丢弃）。
//   - ID 相同、指纹不同、序号更旧或相等：Stale（过期版本，丢弃）。
//   - ID 相同、指纹不同、序号更新：Correction（更正，需合并替换旧版本）。
package dedup

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"time"

	"github.com/feng111627/market-event-pipeline-go/internal/event"
)

// Verdict 是去重判定结果。
type Verdict int

const (
	// New 首次见到该 ID。
	New Verdict = iota
	// Duplicate ID 与内容指纹一致，属重复投递。
	Duplicate
	// Stale 同 ID 但序号更旧或相等且内容不同，属过期版本。
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

// Entry 记录已见事件的指纹与序号，可序列化用于断点。
type Entry struct {
	Fingerprint uint64
	Seq         uint64
}

// Store 保存已见事件的判定状态，容量有界（FIFO 淘汰）以防内存膨胀。
type Store struct {
	entries map[string]Entry
	order   []string // FIFO 淘汰队列
	maxSize int
}

// NewStore 创建容量为 maxSize 的去重存储；超出容量时淘汰最旧条目。
func NewStore(maxSize int) *Store {
	if maxSize <= 0 {
		maxSize = 1
	}
	return &Store{entries: make(map[string]Entry), maxSize: maxSize}
}

// Fingerprint 计算事件内容的稳定指纹（不含 ID 与 Seq）。
// 核心字段与全部已声明扩展字段（Attrs）都参与指纹，因此只改扩展字段的
// 更正版本不会被误判为重复投递。
func Fingerprint(ev event.MarketEvent) uint64 {
	h := fnv.New64a()
	fmt.Fprintf(h, "%s|%f|%f|%d", ev.Symbol, ev.Price, ev.Volume, ev.EventTime.UnixNano())
	if len(ev.Attrs) > 0 {
		keys := make([]string, 0, len(ev.Attrs))
		for k := range ev.Attrs {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var b strings.Builder
		for _, k := range keys {
			b.WriteByte('|')
			b.WriteString(k)
			b.WriteByte('=')
			writeAttrForHash(&b, ev.Attrs[k])
		}
		h.Write([]byte(b.String()))
	}
	return h.Sum64()
}

// writeAttrForHash 以稳定的字符串形式写入扩展字段值。
func writeAttrForHash(b *strings.Builder, v interface{}) {
	switch x := v.(type) {
	case time.Time:
		b.WriteString("t:")
		b.WriteString(x.UTC().Format(time.RFC3339Nano))
	case string:
		b.WriteString("s:")
		b.WriteString(x)
	case float64:
		b.WriteString(fmt.Sprintf("f:%.17g", x))
	case int64:
		b.WriteString(fmt.Sprintf("i:%d", x))
	case bool:
		b.WriteString(fmt.Sprintf("b:%t", x))
	default:
		b.WriteString("?:")
		b.WriteString(fmt.Sprintf("%v", x))
	}
}

// Check 判定事件，并在需要时更新内部状态。
func (s *Store) Check(ev event.MarketEvent) Verdict {
	fp := Fingerprint(ev)
	prev, seen := s.entries[ev.ID]
	if !seen {
		s.put(ev.ID, Entry{Fingerprint: fp, Seq: ev.Seq})
		return New
	}
	switch {
	case prev.Fingerprint == fp:
		return Duplicate
	case ev.Seq <= prev.Seq:
		return Stale
	default:
		s.entries[ev.ID] = Entry{Fingerprint: fp, Seq: ev.Seq}
		return Correction
	}
}

// put 写入条目并执行 FIFO 淘汰。
func (s *Store) put(id string, e Entry) {
	if _, exists := s.entries[id]; !exists {
		s.order = append(s.order, id)
	}
	s.entries[id] = e
	for len(s.order) > s.maxSize {
		oldest := s.order[0]
		s.order = s.order[1:]
		delete(s.entries, oldest)
	}
}

// Snapshot 导出内部状态用于断点持久化。
func (s *Store) Snapshot() map[string]Entry {
	out := make(map[string]Entry, len(s.entries))
	for k, v := range s.entries {
		out[k] = v
	}
	return out
}

// Restore 从断点恢复内部状态（淘汰队列按 map 遍历顺序重建，
// 只影响容量满时的淘汰先后，不影响判定正确性）。
func (s *Store) Restore(snap map[string]Entry) {
	s.entries = make(map[string]Entry, len(snap))
	s.order = s.order[:0]
	for k, v := range snap {
		s.entries[k] = v
		s.order = append(s.order, k)
	}
	for len(s.order) > s.maxSize {
		oldest := s.order[0]
		s.order = s.order[1:]
		delete(s.entries, oldest)
	}
}

// Len 返回当前条目数。
func (s *Store) Len() int {
	return len(s.entries)
}
