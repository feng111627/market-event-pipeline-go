package watermark

import (
	"fmt"
	"testing"
	"time"

	"github.com/feng111627/market-event-pipeline-go/internal/event"
)

var base = time.Date(2026, 10, 6, 10, 0, 0, 0, time.UTC)

func ev(id string, sec int) event.MarketEvent {
	return event.MarketEvent{ID: id, Symbol: "S", EventTime: base.Add(time.Duration(sec) * time.Second)}
}

// TestOutOfOrderDrain 验证乱序事件按事件时间有序输出。
func TestOutOfOrderDrain(t *testing.T) {
	t.Log("业务场景: 乱序到达的事件在缓冲中等待，水位线推进后按事件时间输出")
	tr := NewTracker(5*time.Second, time.Second, 0)
	tr.Add(ev("e3", 13))
	tr.Add(ev("e1", 11)) // 乱序：比 e3 早 2 秒，在容忍窗口内
	tr.Add(ev("e2", 12))

	// 水位线 = 13 - 5 = 8，三条事件时间均 > 8，都应在等待。
	if got := len(tr.DrainReady()); got != 0 {
		t.Fatalf("水位线未越过事件，不应有输出, 得到 %d 条", got)
	}
	t.Log("用例结果: 水位线未越过时事件等待不输出 [通过]")

	tr.Add(ev("e4", 20)) // 推进水位线到 15
	ready := tr.DrainReady()
	if len(ready) != 3 || ready[0].ID != "e1" || ready[1].ID != "e2" || ready[2].ID != "e3" {
		t.Fatalf("输出顺序错误: %v", ids(ready))
	}
	t.Logf("用例结果: 水位线推进后按事件时间输出 %v [通过]", ids(ready))
}

// TestLateDropped 验证超出迟到宽限的事件被判定丢弃。
func TestLateDropped(t *testing.T) {
	t.Log("业务场景: 事件时间落后于水位线减迟到宽限的事件被明确丢弃")
	tr := NewTracker(5*time.Second, 2*time.Second, 0)
	tr.Add(ev("e1", 20)) // 水位线 = 15，迟到截止 = 13
	if !tr.IsLate(ev("e2", 10)) {
		t.Fatal("事件时间 10 应判定为迟到")
	}
	if tr.IsLate(ev("e3", 14)) {
		t.Fatal("事件时间 14 在宽限内，不应判定为迟到")
	}
	t.Log("用例结果: 迟到边界判定正确（截止=水位线-宽限） [通过]")
}

// TestRemoveForCorrection 验证更正合并时可移除缓冲中的旧版本。
func TestRemoveForCorrection(t *testing.T) {
	t.Log("业务场景: 同一 ID 的更正版本到达时，缓冲中的旧版本被替换")
	tr := NewTracker(5*time.Second, time.Second, 0)
	tr.Add(ev("e1", 11))
	if !tr.Remove("e1") {
		t.Fatal("应能移除缓冲中的 e1")
	}
	if tr.Remove("e1") {
		t.Fatal("重复移除应返回 false")
	}
	tr.Add(ev("e1", 12)) // 更正版本
	tr.Add(ev("e2", 20))
	ready := tr.DrainReady()
	if len(ready) != 1 || ready[0].ID != "e1" || !ready[0].EventTime.Equal(base.Add(12*time.Second)) {
		t.Fatalf("合并结果错误: %v", ids(ready))
	}
	t.Log("用例结果: 旧版本被移除，更正版本进入缓冲 [通过]")
}

// TestFlush 验证关停时强制排空。
func TestFlush(t *testing.T) {
	t.Log("业务场景: 源耗尽后强制排空缓冲，剩余事件仍按事件时间输出")
	tr := NewTracker(time.Hour, 0, 0)
	tr.Add(ev("e2", 12))
	tr.Add(ev("e1", 11))
	out := tr.Flush()
	if len(out) != 2 || out[0].ID != "e1" || out[1].ID != "e2" {
		t.Fatalf("Flush 输出错误: %v", ids(out))
	}
	t.Logf("用例结果: Flush 按事件时间输出 %v [通过]", ids(out))
}

func ids(evs []event.MarketEvent) []string {
	out := make([]string, len(evs))
	for i, e := range evs {
		out[i] = e.ID
	}
	return out
}

// TestBoundedBufferForcesOldest 验证乱序缓冲有硬上界：超限时强制输出
// 最旧事件、抬高地板，后续更旧事件按迟到丢弃，输出仍事件时间非降序。
func TestBoundedBufferForcesOldest(t *testing.T) {
	t.Log("业务场景: 持续乱序导致缓冲超过硬上界，最旧事件被强制提前输出，内存不随输入增长")
	tr := NewTracker(time.Hour, 0, 3) // 水位线窗口很大，排除常规迟到，容量仅 3
	forced := 0
	for sec := 0; sec < 10; sec++ {
		// 逆序到达（乱序），事件时间唯一。
		f, ok := tr.Add(ev(fmt.Sprintf("e%02d", sec), 9-sec))
		if ok {
			forced++
			t.Logf("用例结果: 缓冲超限，强制输出事件时间 %s [通过]", f.EventTime.Format("15:04:05"))
		}
	}
	if tr.Buffered() != 3 {
		t.Fatalf("缓冲应恒定在上界 3, 得到 %d", tr.Buffered())
	}
	if forced == 0 {
		t.Fatal("应发生强制输出")
	}
	// 被强制输出过的最旧事件时间之后，更旧的事件必须判迟到（保序地板）。
	floor, valid := tr.EmittedFloor()
	if !valid {
		t.Fatal("应存在已输出地板")
	}
	older := event.MarketEvent{ID: "late", Symbol: "S", EventTime: floor.Add(-time.Second)}
	if !tr.IsLate(older) {
		t.Fatal("早于强制输出地板的事件应判迟到")
	}
	// Flush 剩余事件必须整体非降序且都不早于地板。
	out := tr.Flush()
	var prev = floor
	for _, e := range out {
		if e.EventTime.Before(prev) {
			t.Fatalf("Flush 输出失去顺序: %s < %s", e.EventTime, prev)
		}
		prev = e.EventTime
	}
	t.Logf("用例结果: 强制输出=%d 次，缓冲恒定 3，地板=%s，剩余 %d 条有序排空 [通过]",
		forced, floor.Format("15:04:05"), len(out))
}
