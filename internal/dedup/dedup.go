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
	"strconv"
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
// 除核心行情字段外，扩展字段 Attrs（结构演进新增的已声明字段）也纳入指纹，
// 使"只改扩展字段、不碰核心字段"的版本更新能被正确识别为更正/过期，
// 而不会被误判成重复投递而静默丢失。
func Fingerprint(ev event.MarketEvent) uint64 {
	h := fnv.New64a()
	fmt.Fprintf(h, "%s|%s|%s|%d|", ev.Symbol,
		strconv.FormatFloat(ev.Price, 'g', -1, 64),
		strconv.FormatFloat(ev.Volume, 'g', -1, 64),
		ev.EventTime.UnixNano())
	writeAttrs(h, ev.Attrs)
	return h.Sum64()
}

// writeAttrs 按字段名排序后稳定写入 attrs，保证指纹可复现。
// time.Time 统一用 RFC3339Nano 表示；其余类型按 Go 默认可打印形式。
func writeAttrs(h interface{ Write([]byte) (int, error) }, attrs map[string]interface{}) {
	if len(attrs) == 0 {
		return
	}
	keys := make([]string, 0, len(attrs))
	for k := range attrs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		b.WriteString(k)
		b.WriteByte('=')
		switch v := attrs[k].(type) {
		case time.Time:
			b.WriteString(v.Format(time.RFC3339Nano))
		default:
			fmt.Fprint(&b, v)
		}
		b.WriteByte(';')
	}
	h.Write([]byte(b.String()))
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
