// Package watermark 实现事件时间水位线与乱序重排缓冲。
//
// 语义：
//   - 水位线 = 已见最大事件时间 - 最大乱序容忍（maxOutOfOrderness）。
//   - 事件时间 <= 水位线 - 允许迟到宽限（allowedLateness）的事件判定为迟到，丢弃。
//   - 其余事件进入最小堆缓冲，事件时间 <= 水位线时按序弹出。
package watermark

import (
	"container/heap"
	"time"

	"github.com/feng111627/market-event-pipeline-go/internal/event"
)

// Tracker 跟踪事件时间进度，判定迟到事件并按事件时间顺序输出。
type Tracker struct {
	maxOutOfOrderness time.Duration
	allowedLateness   time.Duration
	maxSeen           time.Time
	hasSeen           bool
	buf               eventHeap
	index             map[string]int // ID -> 堆下标，用于更正合并时定位旧版本
}

// NewTracker 创建水位线跟踪器。
func NewTracker(maxOutOfOrderness, allowedLateness time.Duration) *Tracker {
	return &Tracker{
		maxOutOfOrderness: maxOutOfOrderness,
		allowedLateness:   allowedLateness,
		buf:               eventHeap{},
		index:             make(map[string]int),
	}
}

// Watermark 返回当前水位线：maxSeen - maxOutOfOrderness；未见过事件时返回零值。
func (t *Tracker) Watermark() time.Time {
	if !t.hasSeen {
		return time.Time{}
	}
	return t.maxSeen.Add(-t.maxOutOfOrderness)
}

// IsLate 判定事件是否已超出可容忍的迟到范围（应丢弃）。
func (t *Tracker) IsLate(ev event.MarketEvent) bool {
	if !t.hasSeen {
		return false
	}
	deadline := t.Watermark().Add(-t.allowedLateness)
	return !ev.EventTime.After(deadline)
}

// Add 将事件放入重排缓冲，并推进 maxSeen。
func (t *Tracker) Add(ev event.MarketEvent) {
	if !t.hasSeen || ev.EventTime.After(t.maxSeen) {
		t.maxSeen = ev.EventTime
		t.hasSeen = true
	}
	heap.Push(&t.buf, ev)
	t.reindex()
}

// Remove 从缓冲中移除指定 ID 的事件（用于更正合并时替换旧版本）。
func (t *Tracker) Remove(id string) bool {
	i, ok := t.index[id]
	if !ok {
		return false
	}
	heap.Remove(&t.buf, i)
	t.reindex()
	return true
}

// DrainReady 弹出所有事件时间 <= 当前水位线的事件（按事件时间升序）。
func (t *Tracker) DrainReady() []event.MarketEvent {
	var out []event.MarketEvent
	wm := t.Watermark()
	for t.buf.Len() > 0 && !t.buf[0].EventTime.After(wm) {
		out = append(out, heap.Pop(&t.buf).(event.MarketEvent))
	}
	t.reindex()
	return out
}

// Flush 强制弹出缓冲中全部剩余事件（用于关停时收尾），按事件时间升序。
func (t *Tracker) Flush() []event.MarketEvent {
	out := make([]event.MarketEvent, 0, t.buf.Len())
	for t.buf.Len() > 0 {
		out = append(out, heap.Pop(&t.buf).(event.MarketEvent))
	}
	t.reindex()
	return out
}

// Buffered 返回当前缓冲中的事件数。
func (t *Tracker) Buffered() int {
	return t.buf.Len()
}

// Snapshot 导出缓冲中的事件（用于断点持久化），不保证顺序。
func (t *Tracker) Snapshot() []event.MarketEvent {
	out := make([]event.MarketEvent, len(t.buf))
	copy(out, t.buf)
	return out
}

// RestoreBuffered 从断点恢复重排缓冲内容。
func (t *Tracker) RestoreBuffered(evs []event.MarketEvent) {
	t.buf = append(t.buf[:0], evs...)
	heap.Init(&t.buf)
	t.reindex()
}

// SetMaxSeen 恢复断点时直接设置已见最大事件时间。
func (t *Tracker) SetMaxSeen(ts time.Time) {
	t.maxSeen = ts
	t.hasSeen = true
}

// MaxSeen 返回已见的最大事件时间。
func (t *Tracker) MaxSeen() time.Time {
	return t.maxSeen
}

// reindex 重建 ID -> 堆下标映射。缓冲规模受水位线窗口约束，重建开销可接受。
func (t *Tracker) reindex() {
	t.index = make(map[string]int, len(t.buf))
	for i := range t.buf {
		t.index[t.buf[i].ID] = i
	}
}
