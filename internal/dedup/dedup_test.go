package dedup

import (
	"testing"
	"time"

	"github.com/feng111627/market-event-pipeline-go/internal/event"
)

func ev(id string, seq uint64, price float64) event.MarketEvent {
	return event.MarketEvent{
		ID: id, Symbol: "BTCUSDT", Price: price, Volume: 1,
		Seq: seq, EventTime: time.Date(2026, 10, 6, 10, 0, 0, 0, time.UTC),
	}
}

// TestVerdicts 覆盖四种判定：新事件、重复投递、过期版本、更正合并。
func TestVerdicts(t *testing.T) {
	t.Log("业务场景: 重复投递与版本更正的判定")
	s := NewStore(100)

	if v := s.Check(ev("e1", 1, 100)); v != New {
		t.Fatalf("首次应为 NEW, 得到 %v", v)
	}
	t.Log("用例结果: 首次到达 -> NEW [通过]")

	if v := s.Check(ev("e1", 1, 100)); v != Duplicate {
		t.Fatalf("同 ID 同内容应为 DUPLICATE, 得到 %v", v)
	}
	t.Log("用例结果: 同 ID 同内容重复投递 -> DUPLICATE [通过]")

	if v := s.Check(ev("e1", 0, 999)); v != Stale {
		t.Fatalf("同 ID 更旧序号应为 STALE, 得到 %v", v)
	}
	t.Log("用例结果: 同 ID 更旧序号且内容不同 -> STALE [通过]")

	if v := s.Check(ev("e1", 2, 101)); v != Correction {
		t.Fatalf("同 ID 更新序号应为 CORRECTION, 得到 %v", v)
	}
	t.Log("用例结果: 同 ID 更新序号且内容不同 -> CORRECTION [通过]")

	// 更正后再收到旧版本：应为 STALE（状态已推进到 seq=2）。
	if v := s.Check(ev("e1", 1, 100)); v != Stale {
		t.Fatalf("更正后旧版本应为 STALE, 得到 %v", v)
	}
	t.Log("用例结果: 更正后旧版本重放 -> STALE [通过]")
}

// TestCapacityEviction 验证容量有界，防止内存膨胀。
func TestCapacityEviction(t *testing.T) {
	t.Log("业务场景: 突发流量下去重表容量有界，FIFO 淘汰最旧条目")
	s := NewStore(3)
	s.Check(ev("a", 1, 1))
	s.Check(ev("b", 1, 1))
	s.Check(ev("c", 1, 1))
	s.Check(ev("d", 1, 1)) // 淘汰 a
	if s.Len() != 3 {
		t.Fatalf("容量 = %d, 期望 3", s.Len())
	}
	if v := s.Check(ev("a", 1, 1)); v != New {
		t.Fatalf("被淘汰的 a 应重新判定为 NEW, 得到 %v", v)
	}
	t.Log("用例结果: 容量恒定 3，最旧条目被淘汰 [通过]")
}

// TestSnapshotRestore 验证去重状态可随断点持久化与恢复。
func TestSnapshotRestore(t *testing.T) {
	t.Log("业务场景: 崩溃恢复后去重状态不丢失，重复投递仍被识别")
	s := NewStore(100)
	s.Check(ev("e1", 1, 100))
	snap := s.Snapshot()

	s2 := NewStore(100)
	s2.Restore(snap)
	if v := s2.Check(ev("e1", 1, 100)); v != Duplicate {
		t.Fatalf("恢复后重复投递应为 DUPLICATE, 得到 %v", v)
	}
	t.Log("用例结果: 快照恢复后重复判定保持一致 [通过]")
}

// TestAttrsAffectVerdict 验证扩展字段参与指纹：只改扩展字段、seq 更新的
// 更正版本必须判为 CORRECTION 而不是 DUPLICATE。
func TestAttrsAffectVerdict(t *testing.T) {
	t.Log("业务场景: 上游只修改了新增扩展字段（更正），去重判定必须感知 Attrs")
	s := NewStore(100)
	base := ev("e1", 1, 100)
	base.Attrs = map[string]interface{}{"exchange": "A", "is_maker": false}
	if v := s.Check(base); v != New {
		t.Fatalf("首次应为 NEW, 得到 %v", v)
	}
	// 完全一致（含 attrs）-> 重复。
	dup := ev("e1", 1, 100)
	dup.Attrs = map[string]interface{}{"exchange": "A", "is_maker": false}
	if v := s.Check(dup); v != Duplicate {
		t.Fatalf("核心与扩展字段都一致应为 DUPLICATE, 得到 %v", v)
	}
	t.Log("用例结果: 核心+扩展字段完全一致 -> DUPLICATE [通过]")
	// 只改扩展字段且 seq 更新 -> 更正。
	corr := ev("e1", 2, 100)
	corr.Attrs = map[string]interface{}{"exchange": "B", "is_maker": true}
	if v := s.Check(corr); v != Correction {
		t.Fatalf("仅扩展字段变化且 seq 更新应为 CORRECTION, 得到 %v", v)
	}
	t.Log("用例结果: 仅扩展字段变化且 seq 更新 -> CORRECTION（指纹纳入 Attrs） [通过]")
}
