package pipeline

import (
	"testing"
	"time"

	"github.com/feng111627/market-event-pipeline-go/internal/event"
	"github.com/feng111627/market-event-pipeline-go/internal/schema"
)

func mustReg(t *testing.T) *schema.Registry {
	t.Helper()
	r := schema.NewRegistry()
	if err := r.Register(1, nil); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := r.Register(2, []schema.FieldDef{
		{Name: "bid_size", Kind: schema.KindInt64, Required: false, Default: 0, HasDefault: true},
	}); err != nil {
		t.Fatalf("register v2: %v", err)
	}
	return r
}

func dec(t *testing.T, r *schema.Registry, line string) *schema.DecodedRecord {
	t.Helper()
	rec, err := r.Decode(line)
	if err != nil {
		t.Fatalf("decode %q: %v", line, err)
	}
	return rec
}

var base = time.Date(2026, 10, 6, 10, 0, 0, 0, time.UTC)

func line(seq, sym string, price, qty int64, sec int64) string {
	return `{"schema_v":1,"seq_id":"` + seq + `","symbol":"` + sym + `","price":` +
		itoa(price) + `,"qty":` + itoa(qty) + `,"event_time":"2026-10-06T10:00:` + pad2(sec) + `Z"}`
}

func wmCtrl(sec int64) time.Time { return base.Add(time.Duration(sec) * time.Second) }

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

func pad2(n int64) string {
	if n < 10 {
		return "0" + itoa(n)
	}
	return itoa(n)
}

func TestProcessor_OutOfOrderSortedByEventTime(t *testing.T) {
	t.Log("【业务覆盖】乱序到达：到达序 s1,s3,s2，水位推进后必须按事件时间 s1<s2<s3 输出")
	r := mustReg(t)
	p := NewProcessor(2 * time.Second)
	now := time.Now()

	p.Ingest(dec(t, r, line("s1", "X", 100, 1, 0)), 0, now)
	p.Ingest(dec(t, r, line("s3", "X", 102, 1, 2)), 0, now)
	p.Ingest(dec(t, r, line("s2", "X", 101, 1, 1)), 0, now)
	if p.BufferLen() != 3 {
		t.Fatalf("want 3 buffered, got %d", p.BufferLen())
	}
	out, err := p.AdvanceWatermark(wmCtrl(3))
	if err != nil {
		t.Fatal(err)
	}
	got := []string{}
	for _, e := range out {
		got = append(got, e.Ev.SeqID)
	}
	want := []string{"s1", "s2", "s3"}
	if len(got) != 3 || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Fatalf("event-time order want %v, got %v", want, got)
	}
	t.Logf("  ✔ 输出顺序 %v，价格依次 %d,%d,%d", got, out[0].Ev.Price, out[1].Ev.Price, out[2].Ev.Price)
}

func TestProcessor_DuplicateNotCountedTwice(t *testing.T) {
	t.Log("【业务覆盖】重复投递：相同 seq_id+相同内容 -> duplicate，绝不第二次计入结果")
	r := mustReg(t)
	p := NewProcessor(2 * time.Second)
	now := time.Now()
	l := line("s1", "X", 100, 1, 0)

	d1, _ := p.Ingest(dec(t, r, l), 10, now)
	if d1.Reason != event.ReasonBuffered {
		t.Fatalf("first want buffered, got %s", d1.Reason)
	}
	out, _ := p.AdvanceWatermark(wmCtrl(3))
	if len(out) != 1 {
		t.Fatalf("want 1 emitted, got %d", len(out))
	}
	// 在判重保留窗口内重投同一条
	d2, out2 := p.Ingest(dec(t, r, l), 40, now)
	if d2.Reason != event.ReasonDuplicate {
		t.Fatalf("redelivery want duplicate, got %s (%s)", d2.Reason, d2.Detail)
	}
	if len(out2) != 0 {
		t.Fatalf("duplicate must not emit again, got %d", len(out2))
	}
	t.Logf("  ✔ 重投被判定 reason=%s，detail=%s", d2.Reason, d2.Detail)
	t.Logf("  ✔ 输出总数仍为 1，序号连续")
}

func TestProcessor_SameIDDifferentContentConflict(t *testing.T) {
	t.Log("【业务覆盖】seq_id 复用但内容不同 -> conflict，明确拒绝而非当重复或覆盖")
	r := mustReg(t)
	p := NewProcessor(2 * time.Second)
	now := time.Now()
	p.Ingest(dec(t, r, line("s1", "X", 100, 1, 0)), 0, now)
	d, _ := p.Ingest(dec(t, r, line("s1", "X", 999, 1, 0)), 0, now)
	if d.Reason != event.ReasonConflict {
		t.Fatalf("want conflict, got %s", d.Reason)
	}
	t.Logf("  ✔ 同 seq_id 不同价格 -> reason=%s, detail=%s", d.Reason, d.Detail)
}

func TestProcessor_LateEventDroppedWithReason(t *testing.T) {
	t.Log("【业务覆盖】晚到：水位越过事件时间后才首次出现的新事件 -> late_drop，明确丢弃")
	r := mustReg(t)
	p := NewProcessor(2 * time.Second)
	now := time.Now()
	// 先让水位到达 10:00:05
	p.Ingest(dec(t, r, line("s5", "X", 105, 1, 5)), 0, now)
	out, _ := p.AdvanceWatermark(wmCtrl(5))
	if len(out) != 1 {
		t.Fatalf("setup emit want 1, got %d", len(out))
	}
	// 全新 seq_id，事件时间 10:00:00 < watermark
	d, out2 := p.Ingest(dec(t, r, line("old1", "X", 1, 1, 0)), 0, now)
	if d.Reason != event.ReasonLateDrop {
		t.Fatalf("want late_drop, got %s", d.Reason)
	}
	if len(out2) != 0 {
		t.Fatalf("late event must not be emitted")
	}
	t.Logf("  ✔ reason=%s, detail=%s", d.Reason, d.Detail)
}

func TestProcessor_MergePartialArrivals(t *testing.T) {
	t.Log("【业务覆盖】部分字段到达：同 symbol+event_time 的两条互补记录合并为一条结果")
	r := mustReg(t)
	p := NewProcessor(2 * time.Second)
	now := time.Now()

	// 第一条：只有 price（qty 为 0 不合法——schema 要求 qty 必填），
	// 因此合并演示改为：两条字段齐全、同 symbol 同 event_time 的“同一报价的分片”，
	// 用 v2 的 extra.bid_size 演示互补字段：第一条缺 bid_size(0)，第二条补 30。
	l1 := `{"schema_v":2,"seq_id":"m1","symbol":"X","price":100,"qty":5,"event_time":"2026-10-06T10:00:00Z","extra":{"bid_size":0}}`
	l2 := `{"schema_v":2,"seq_id":"m2","symbol":"X","price":100,"qty":5,"event_time":"2026-10-06T10:00:00Z","extra":{"bid_size":30}}`

	d1, _ := p.Ingest(dec(t, r, l1), 0, now)
	if d1.Reason != event.ReasonBuffered {
		t.Fatalf("m1 want buffered, got %s", d1.Reason)
	}
	d2, _ := p.Ingest(dec(t, r, l2), 0, now)
	if d2.Reason != event.ReasonMerged {
		t.Fatalf("m2 want merged, got %s (%s)", d2.Reason, d2.Detail)
	}
	out, _ := p.AdvanceWatermark(wmCtrl(3))
	if len(out) != 1 {
		t.Fatalf("coalesced result want 1, got %d", len(out))
	}
	if out[0].Ev.Extra["bid_size"] != 30 {
		t.Fatalf("merged bid_size want 30, got %d", out[0].Ev.Extra["bid_size"])
	}
	if !out[0].Merged {
		t.Fatalf("emitted should be flagged Merged")
	}
	t.Logf("  ✔ %s；合并后 bid_size=30，仅输出 1 条（不重复计入）", d2.Detail)
}

func TestProcessor_MergeConflictRejected(t *testing.T) {
	t.Log("【业务覆盖】合并冲突：同一报价位上两个不一致价格 -> conflict，拒绝")
	r := mustReg(t)
	p := NewProcessor(2 * time.Second)
	now := time.Now()
	p.Ingest(dec(t, r, line("m1", "X", 100, 5, 0)), 0, now)
	d, _ := p.Ingest(dec(t, r, line("m2", "X", 101, 5, 0)), 0, now)
	if d.Reason != event.ReasonConflict {
		t.Fatalf("want merge conflict, got %s (%s)", d.Reason, d.Detail)
	}
	t.Logf("  ✔ reason=%s, detail=%s", d.Reason, d.Detail)
}

func TestProcessor_WatermarkNeverRegresses(t *testing.T) {
	t.Log("【业务覆盖】水位只进不退：倒退的 watermark 控制必须报错拒绝")
	p := NewProcessor(2 * time.Second)
	if _, err := p.AdvanceWatermark(wmCtrl(5)); err != nil {
		t.Fatal(err)
	}
	if _, err := p.AdvanceWatermark(wmCtrl(3)); err == nil {
		t.Fatal("watermark regression must be rejected")
	} else {
		t.Logf("  ✔ 水位回退被拒绝: %v", err)
	}
}

func TestProcessor_SnapshotRestoreRoundTrip(t *testing.T) {
	t.Log("【业务覆盖】断点序列化/恢复：缓冲中的在途记录与去重表必须随断点保留")
	r := mustReg(t)
	p := NewProcessor(2 * time.Second)
	now := time.Now()
	p.Ingest(dec(t, r, line("s1", "X", 100, 1, 0)), 0, now)
	p.Ingest(dec(t, r, line("s2", "X", 101, 1, 1)), 0, now)

	raw, err := p.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	p2 := NewProcessor(2 * time.Second)
	if err := p2.Restore(raw); err != nil {
		t.Fatal(err)
	}
	if p2.BufferLen() != 2 {
		t.Fatalf("restored pending want 2, got %d", p2.BufferLen())
	}
	// 恢复后重投 s1 仍应判重
	d, _ := p2.Ingest(dec(t, r, line("s1", "X", 100, 1, 0)), 0, now)
	if d.Reason != event.ReasonDuplicate {
		t.Fatalf("after restore redelivery want duplicate, got %s", d.Reason)
	}
	out, _ := p2.AdvanceWatermark(wmCtrl(3))
	if len(out) != 2 || out[0].Ev.SeqID != "s1" || out[1].Ev.SeqID != "s2" {
		t.Fatalf("restored flush order wrong: %+v", out)
	}
	t.Log("  ✔ 恢复后 pending=2、重投 s1 判重、水位推进后顺序输出 s1,s2")
}
