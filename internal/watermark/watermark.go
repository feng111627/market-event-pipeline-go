// Package watermark 实现事件时间水位线与乱序重排缓冲。
//
// 语义：
//   - 水位线 = 已见最大事件时间 - 最大乱序容忍（maxOutOfOrderness）。
//   - 事件时间 <= 水位线 - 允许迟到宽限（allowedLateness）的事件判定为迟到，丢弃。
//   - 其余事件进入最小堆缓冲，事件时间 <= 水位线时按序弹出。
//   - 缓冲容量有硬上界（maxBuffer）：超限时最小事件时间的事件被强制提前
//     输出，并抬高"已输出地板"，使内存与输入规模解耦且输出仍按事件时间非降序。
package watermark

import (
	"time"

	"github.com/feng111627/market-event-pipeline-go/internal/event"
)

// Tracker 跟踪事件时间进度，判定迟到事件并按事件时间顺序输出。
type Tracker struct {
	maxOutOfOrderness time.Duration
	allowedLateness   time.Duration
	maxBuffer         int // 乱序缓冲容量硬上界（<=0 表示不限）
	maxSeen           time.Time
	hasSeen           bool
	emittedFloor      time.Time // 因缓冲上界强制输出过的最大事件时间
	hasFloor          bool
	buf               *indexedHeap
}

// NewTracker 创建水位线跟踪器。
// maxBuffer 为乱序缓冲容量硬上界（<=0 表示不限）；缓冲超限时最旧事件
// （最小事件时间）会被强制提前输出，内存占用因此与输入规模解耦。
func NewTracker(maxOutOfOrderness, allowedLateness time.Duration, maxBuffer int) *Tracker {
	return &Tracker{
		maxOutOfOrderness: maxOutOfOrderness,
		allowedLateness:   allowedLateness,
		maxBuffer:         maxBuffer,
		buf:               newIndexedHeap(),
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
// 同时不允许事件时间早于因缓冲上界强制输出的"地板"，从而保证即使
// 发生强制提前输出，最终输出仍是事件时间非降序。
func (t *Tracker) IsLate(ev event.MarketEvent) bool {
	if !t.hasSeen {
		return false
	}
	if t.hasFloor && ev.EventTime.Before(t.emittedFloor) {
		return true
	}
	deadline := t.Watermark().Add(-t.allowedLateness)
	return !ev.EventTime.After(deadline)
}

// Add 将事件放入重排缓冲，并推进 maxSeen。
// 若缓冲超过容量上界，弹出最旧事件作为强制输出返回（ok=true），
// 同时抬高已输出地板，保证输出事件时间非降序。
func (t *Tracker) Add(ev event.MarketEvent) (forced event.MarketEvent, ok bool) {
	if !t.hasSeen || ev.EventTime.After(t.maxSeen) {
		t.maxSeen = ev.EventTime
		t.hasSeen = true
	}
	t.buf.push(ev)
	if t.maxBuffer > 0 && t.buf.Len() > t.maxBuffer {
		oldest := t.buf.pop()
		if !t.hasFloor || oldest.EventTime.After(t.emittedFloor) {
			t.emittedFloor = oldest.EventTime
			t.hasFloor = true
		}
		return oldest, true
	}
	return event.MarketEvent{}, false
}

// Remove 从缓冲中移除指定 ID 的事件（用于更正合并时替换旧版本）。
func (t *Tracker) Remove(id string) bool {
	i, ok := t.buf.indexOf(id)
	if !ok {
		return false
	}
	t.buf.removeAt(i)
	return true
}

// DrainReady 弹出所有事件时间 <= 当前水位线的事件（按事件时间升序）。
func (t *Tracker) DrainReady() []event.MarketEvent {
	var out []event.MarketEvent
	wm := t.Watermark()
	for t.buf.Len() > 0 && !t.buf.items[0].EventTime.After(wm) {
		out = append(out, t.buf.pop())
	}
	return out
}

// Flush 强制弹出缓冲中全部剩余事件（用于关停时收尾），按事件时间升序。
func (t *Tracker) Flush() []event.MarketEvent {
	out := make([]event.MarketEvent, 0, t.buf.Len())
	for t.buf.Len() > 0 {
		out = append(out, t.buf.pop())
	}
	return out
}

// Buffered 返回当前缓冲中的事件数。
func (t *Tracker) Buffered() int { return t.buf.Len() }

// Snapshot 导出缓冲中的事件（用于断点持久化），不保证顺序。
func (t *Tracker) Snapshot() []event.MarketEvent { return t.buf.snapshot() }

// RestoreBuffered 从断点恢复重排缓冲内容。
func (t *Tracker) RestoreBuffered(evs []event.MarketEvent) { t.buf.init(evs) }

// SetMaxSeen 恢复断点时直接设置已见最大事件时间。
func (t *Tracker) SetMaxSeen(ts time.Time) {
	t.maxSeen = ts
	t.hasSeen = true
}

// MaxSeen 返回已见的最大事件时间。
func (t *Tracker) MaxSeen() time.Time { return t.maxSeen }

// EmittedFloor 返回因缓冲上界强制输出过的最大事件时间（零值表示未发生过）。
func (t *Tracker) EmittedFloor() (time.Time, bool) {
	return t.emittedFloor, t.hasFloor
}

// SetEmittedFloor 恢复断点时还原因缓冲上界强制输出形成的地板。
func (t *Tracker) SetEmittedFloor(ts time.Time, valid bool) {
	t.emittedFloor = ts
	t.hasFloor = valid
}
