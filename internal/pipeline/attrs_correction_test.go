package pipeline

import (
	"context"
	"testing"
)

// vRec 构造一条带扩展字段 exchange 的 V2 记录。
func vRec(offset int64, version int, id string, seq int64, sec int, exchange string) SourceRecord {
	r := rec(offset, version, id, seq, sec, 100)
	if exchange != "" {
		r.Payload["exchange"] = exchange
	}
	return r
}

// TestAttrsOnlyCorrectionMerged 验证：同一 ID、核心字段完全不变、只更新扩展字段
// 且序号更新时，必须判为合并更正（MERGED），新字段值得以到达下游；
// 序号相同/更旧则判为过期（STALE），内容指纹一致则为重复（DUPLICATE）。
func TestAttrsOnlyCorrectionMerged(t *testing.T) {
	t.Log("业务场景: 更正只动新增字段 exchange、不碰核心字段，仍应合并到下游而不是被当重复丢弃")
	cfg := testConfig(t)
	p := New(cfg, testRegistry(t))
	src := &sliceSource{recs: []SourceRecord{
		vRec(0, 2, "e1", 1, 11, "BINANCE"),
		vRec(1, 2, "e1", 1, 11, "BINANCE"),  // 完全一致：重复
		vRec(2, 2, "e1", 1, 11, "COINBASE"), // 字段变了但 seq 相同：过期
		vRec(3, 2, "e1", 2, 11, "COINBASE"), // 字段变了且 seq 更新：合并更正
		vRec(4, 1, "e9", 1, 30, ""),         // 推进水位线，排空缓冲
	}}
	sink := newMemSink()
	if err := p.Run(context.Background(), src, sink); err != nil {
		t.Fatalf("Run 失败: %v", err)
	}
	st := p.Stats()
	if st.Duplicates != 1 || st.Stales != 1 || st.Merged != 1 {
		t.Fatalf("处置统计不符合预期: %+v", st)
	}
	if len(sink.events) != 2 {
		t.Fatalf("输出数 = %d, 期望 2", len(sink.events))
	}
	got := sink.events[0]
	if got.ID != "e1" || got.Attrs["exchange"] != "COINBASE" {
		t.Fatalf("合并后 e1 的扩展字段应为 COINBASE: %+v", got.Attrs)
	}
	t.Logf("用例结果: 仅扩展字段更新被识别为 MERGED，下游拿到最新 exchange=COINBASE [通过] stats=%+v", st)
}
