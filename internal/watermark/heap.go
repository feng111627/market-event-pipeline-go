package watermark

import (
	"container/heap"

	"github.com/feng111627/market-event-pipeline-go/internal/event"
)

// indexedHeap 是按事件时间排序的最小堆，并增量维护 ID -> 堆下标映射，
// 避免在大缓冲下每次增删都全量重建索引。
type indexedHeap struct {
	items []event.MarketEvent
	index map[string]int
}

func newIndexedHeap() *indexedHeap {
	return &indexedHeap{index: make(map[string]int)}
}

func (h *indexedHeap) Len() int { return len(h.items) }

func (h *indexedHeap) Less(i, j int) bool {
	return h.items[i].EventTime.Before(h.items[j].EventTime)
}

func (h *indexedHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.index[h.items[i].ID] = i
	h.index[h.items[j].ID] = j
}

// Push 仅供 container/heap 调用；请使用 push 以同步索引。
func (h *indexedHeap) Push(x interface{}) {
	ev := x.(event.MarketEvent)
	h.index[ev.ID] = len(h.items)
	h.items = append(h.items, ev)
}

// Pop 仅供 container/heap 调用；请使用 pop 以同步索引。
func (h *indexedHeap) Pop() interface{} {
	old := h.items
	n := len(old)
	it := old[n-1]
	h.items = old[:n-1]
	delete(h.index, it.ID)
	return it
}

// push 推入事件并维护索引。
func (h *indexedHeap) push(ev event.MarketEvent) { heap.Push(h, ev) }

// pop 弹出最小事件并维护索引。
func (h *indexedHeap) pop() event.MarketEvent { return heap.Pop(h).(event.MarketEvent) }

// removeAt 删除指定下标的事件并维护索引。
func (h *indexedHeap) removeAt(i int) event.MarketEvent { return heap.Remove(h, i).(event.MarketEvent) }

// indexOf 返回某 ID 的堆下标。
func (h *indexedHeap) indexOf(id string) (int, bool) { i, ok := h.index[id]; return i, ok }

// init 从无序切片建堆并重建索引（仅恢复快照时使用）。
func (h *indexedHeap) init(items []event.MarketEvent) {
	h.items = append(h.items[:0], items...)
	h.index = make(map[string]int, len(h.items))
	for i := range h.items {
		h.index[h.items[i].ID] = i
	}
	heap.Init(h)
}

// snapshot 复制堆内事件（用于断点持久化）。
func (h *indexedHeap) snapshot() []event.MarketEvent {
	out := make([]event.MarketEvent, len(h.items))
	copy(out, h.items)
	return out
}
