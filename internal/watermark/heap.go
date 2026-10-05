package watermark

import (
	"github.com/feng111627/market-event-pipeline-go/internal/event"
)

// eventHeap 是按事件时间排序的最小堆（container/heap 接口）。
type eventHeap []event.MarketEvent

func (h eventHeap) Len() int            { return len(h) }
func (h eventHeap) Less(i, j int) bool  { return h[i].EventTime.Before(h[j].EventTime) }
func (h eventHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *eventHeap) Push(x interface{}) { *h = append(*h, x.(event.MarketEvent)) }
func (h *eventHeap) Pop() interface{} {
	old := *h
	n := len(old)
	it := old[n-1]
	*h = old[:n-1]
	return it
}
