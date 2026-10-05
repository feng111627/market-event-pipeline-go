// Package watermark 实现事件时间水位线与乱序重排缓冲。
package watermark

import (
	"time"

	"github.com/feng111627/market-event-pipeline-go/internal/event"
)

// Tracker 跟踪事件时间进度，判定迟到事件并按事件时间顺序输出。
type Tracker struct {
	maxOutOfOrderness time.Duration // 允许的最大乱序时长
	allowedLateness   time.Duration // 水位线之后仍允许的迟到宽限
	maxSeen           time.Time     // 已见的最大事件时间
	heap              eventHeap     // 按事件时间排序的缓冲（最小堆）
}

// NewTracker 创建水位线跟踪器。
func NewTracker(maxOutOfOrderness, allowedLateness time.Duration) *Tracker {
	return &Tracker{maxOutOfOrderness: maxOutOfOrderness, allowedLateness: allowedLateness}
}

// Watermark 返回当前水位线：maxSeen - maxOutOfOrderness。
func (t *Tracker) Watermark() time.Time {
	return time.Time{}
}

// IsLate 判定事件是否已超出可容忍的迟到范围（应丢弃）。
func (t *Tracker) IsLate(ev event.MarketEvent) bool {
	return false
}

// Add 将事件放入重排缓冲，并推进 maxSeen。
func (t *Tracker) Add(ev event.MarketEvent) {}

// Remove 从缓冲中移除指定 ID 的事件（用于更正合并时替换旧版本）。
func (t *Tracker) Remove(id string) bool {
	return false
}

// DrainReady 弹出所有事件时间 <= 当前水位线的事件（按事件时间升序）。
func (t *Tracker) DrainReady() []event.MarketEvent {
	return nil
}

// Flush 强制弹出缓冲中全部剩余事件（用于关停时收尾）。
func (t *Tracker) Flush() []event.MarketEvent {
	return nil
}

// Buffered 返回当前缓冲中的事件数。
func (t *Tracker) Buffered() int {
	return 0
}

// SetMaxSeen 恢复断点时直接设置已见最大事件时间。
func (t *Tracker) SetMaxSeen(ts time.Time) {}

// MaxSeen 返回已见的最大事件时间。
func (t *Tracker) MaxSeen() time.Time {
	return time.Time{}
}
