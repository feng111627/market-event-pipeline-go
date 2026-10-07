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
	maxBuffered       int            // 缓冲硬上界（>0 生效）；溢出时强制按序弹出最旧事件
	emitFloor         time.Time      // 溢出提前输出已越过的最大事件时间，之后不晚于它的事件按迟到处理
	hasFloor          bool
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

// SetMaxBuffered 设置乱序缓冲的硬上界（<=0 表示不限制）。
// 超出上界时 DrainOverflow 会按事件时间升序强制弹出最旧事件直接输出，
// 使内存占用与输入规模解耦；被提前越过的事件时间记入输出下限，
// 之后到达且不晚于该下限的事件判为迟到，保证全局顺序不被破坏。
func (t *Tracker) SetMaxBuffered(n int) { t.maxBuffered = n }

// Watermark 返回当前水位线：maxSeen - maxOutOfOrderness；未见过事件时返回零值。
func (t *Tracker) Watermark() time.Time {
	if !t.hasSeen {
		return time.Time{}
	}
	return t.maxSeen.Add(-t.maxOutOfOrderness)
}

// IsLate 判定事件是否已超出可容忍的迟到范围（应丢弃）。
// 除水位线迟到外，事件时间不晚于溢出提前输出下限的也判为迟到，
// 否则它会排到已输出事件之后，破坏全局事件时间顺序。
func (t *Tracker) IsLate(ev event.MarketEvent) bool {
	if t.hasSeen {
		deadline := t.Watermark().Add(-t.allowedLateness)
		if !ev.EventTime.After(deadline) {
			return true
		}
	}
	if t.hasFloor && !ev.EventTime.After(t.emitFloor) {
		return true
	}
	return false
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

// DrainOverflow 在缓冲超过硬上界时，按事件时间升序强制弹出最旧的若干事件
// 直接输出（哪怕它们尚未等到水位线），使缓冲内存保持在上界以内。
// 被提前越过的最大事件时间记入输出下限：此后不晚于它到达的事件将被判迟到，
// 从而保证已输出序列的全局事件时间顺序不被后续到达者破坏。
// 未设置上界或未超限时返回空切片，行为与原有缓冲逻辑完全一致。
func (t *Tracker) DrainOverflow() []event.MarketEvent {
	if t.maxBuffered <= 0 || t.buf.Len() <= t.maxBuffered {
		return nil
	}
	out := make([]event.MarketEvent, 0, t.buf.Len()-t.maxBuffered)
	for t.buf.Len() > t.maxBuffered {
		ev := heap.Pop(&t.buf).(event.MarketEvent)
		if !t.hasFloor || ev.EventTime.After(t.emitFloor) {
			t.emitFloor = ev.EventTime
			t.hasFloor = true
		}
		out = append(out, ev)
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
