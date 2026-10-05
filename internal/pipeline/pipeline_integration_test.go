package pipeline

import (
	"bytes"
	"context"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/feng111627/market-event-pipeline-go/internal/checkpoint"
	"github.com/feng111627/market-event-pipeline-go/internal/event"
	"github.com/feng111627/market-event-pipeline-go/internal/ingress"
	"github.com/feng111627/market-event-pipeline-go/internal/schema"
	"github.com/feng111627/market-event-pipeline-go/internal/sink"
)

// fakeClock 可控时钟。
type fakeClock struct{ t time.Time }

func (c *fakeClock) Now() time.Time { return c.t }

// memCP 内存断点存储（测试用）。
type memCP struct {
	mu    sync.Mutex
	s     *checkpoint.State
	saves int
}

func (m *memCP) Load(_ context.Context) (*checkpoint.State, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.s == nil {
		return nil, checkpoint.ErrNoCheckpoint
	}
	cp := *m.s
	return &cp, nil
}

func (m *memCP) Save(_ context.Context, s checkpoint.State) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := s
	m.s = &cp
	m.saves++
	return nil
}

func testRegistry(t *testing.T) *schema.Registry {
	t.Helper()
	r := schema.NewRegistry()
	if err := r.Register(1, nil); err != nil {
		t.Fatal(err)
	}
	if err := r.Register(2, []schema.FieldDef{
		{Name: "bid_size", Kind: schema.KindInt64, Required: false, Default: 0, HasDefault: true},
	}); err != nil {
		t.Fatal(err)
	}
	return r
}

func runOnce(t *testing.T, input string, sk sink.Sink, cp checkpoint.Store, restore *checkpoint.State, bufLimit int, epoch int64) (Stats, *Pipeline) {
	t.Helper()
	clock := &fakeClock{t: time.Date(2026, 10, 6, 10, 0, 0, 0, time.UTC)}
	logger := log.New(&testWriter{t: t}, "    ", 0)
	cfg := Config{
		AllowedLateness:   2 * time.Second,
		BufferLimit:       bufLimit,
		InputQueue:        4,
		CheckpointEvery:   0,
		CheckpointTTL:     0,
		Epoch:             epoch,
		WatermarkInterval: 0, // 测试中只用显式 watermark，保证确定性
	}
	p := New(cfg, testRegistry(t), sk, cp, clock, logger)
	if restore != nil {
		if err := p.Restore(restore); err != nil {
			t.Fatalf("restore: %v", err)
		}
	}
	reader := ingress.NewReader(strings.NewReader(input))
	stats, err := p.Run(context.Background(), reader)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	return stats, p
}

type testWriter struct{ t *testing.T }

func (w *testWriter) Write(p []byte) (int, error) {
	w.t.Logf("%s", strings.TrimRight(string(p), "\n"))
	return len(p), nil
}

func rec1(seq, sym string, price, qty int64, sec int) string {
	return mkLine(1, seq, sym, price, qty, sec, "")
}

func rec2(seq, sym string, price, qty int64, sec int, bidSize int64) string {
	return mkLine(2, seq, sym, price, qty, sec, `"extra":{"bid_size":`+itoa(bidSize)+"}")
}

func mkLine(v int, seq, sym string, price, qty int64, sec int, extra string) string {
	et := time.Date(2026, 10, 6, 10, 0, sec, 0, time.UTC).Format(time.RFC3339Nano)
	var b bytes.Buffer
	b.WriteString(`{"schema_v":`)
	b.WriteString(itoa(int64(v)))
	b.WriteString(`,"seq_id":"` + seq + `","symbol":"` + sym + `","price":`)
	b.WriteString(itoa(price))
	b.WriteString(`,"qty":`)
	b.WriteString(itoa(qty))
	b.WriteString(`,"event_time":"` + et + `"`)
	if extra != "" {
		b.WriteString("," + extra)
	}
	b.WriteString("}")
	return b.String()
}

func ctrl(sec int) string {
	et := time.Date(2026, 10, 6, 10, 0, sec, 0, time.UTC).Format(time.RFC3339Nano)
	return `{"type":"watermark","watermark":"` + et + `"}`
}

// TestIntegration_OutOfOrderDuplicateEvolution 全流程综合场景。
func TestIntegration_OutOfOrderDuplicateEvolution(t *testing.T) {
	t.Log("【业务覆盖】综合：乱序 + 重复投递 + 结构演进(v1/v2 缺省) + 晚到 + 结构拒绝，按事件时间输出")
	var in strings.Builder
	in.WriteString(rec1("s1", "X", 100, 1, 0) + "\n")
	in.WriteString(rec1("s3", "X", 102, 1, 2) + "\n") // 乱序：s3 早于 s2
	in.WriteString(rec1("s2", "X", 101, 1, 1) + "\n")
	in.WriteString(ctrl(3) + "\n")                        // 水位 -> :03，s1,s2,s3 到点
	in.WriteString(rec1("s1", "X", 100, 1, 0) + "\n")     // 重复投递
	in.WriteString(rec1("old", "X", 1, 1, 0) + "\n")      // 全新但晚到 -> late_drop
	in.WriteString(rec2("s4", "Y", 2000, 2, 4, 7) + "\n") // v2 新字段
	in.WriteString(rec1("s5", "X", 105, 1, 5) + "\n")
	in.WriteString(`{"schema_v":1,"seq_id":"bad"}` + "\n")                                                                   // 结构非法 -> schema_rejected
	in.WriteString(`{"schema_v":42,"seq_id":"x","symbol":"X","price":1,"qty":1,"event_time":"2026-10-06T10:00:06Z"}` + "\n") // 未注册版本
	in.WriteString(ctrl(7) + "\n")

	sk := &sink.MemorySink{}
	cp := &memCP{}
	stats, _ := runOnce(t, in.String(), sk, cp, nil, 1000, 1)

	if stats.Accepted != 5 {
		t.Fatalf("accepted want 5 (s1..s5), got %d", stats.Accepted)
	}
	if stats.Duplicate != 1 {
		t.Fatalf("duplicate want 1, got %d", stats.Duplicate)
	}
	if stats.DroppedLate != 1 {
		t.Fatalf("late drop want 1, got %d", stats.DroppedLate)
	}
	if stats.SchemaRejected != 2 {
		t.Fatalf("schema rejected want 2, got %d", stats.SchemaRejected)
	}
	items := sk.Items()
	// 输出顺序：accepted 按事件时间 s1,s2,s3；late_drop(s1 dup? no) old; v2 s4; s5; + 2 schema
	var acceptedIDs []string
	var sawV2Default bool
	for _, o := range items {
		if o.Reason == event.ReasonAccepted {
			acceptedIDs = append(acceptedIDs, o.SeqID)
		}
		if o.SeqID == "s4" && o.Extra["bid_size"] == 7 {
			sawV2Default = true
		}
	}
	wantIDs := []string{"s1", "s2", "s3", "s4", "s5"}
	if strings.Join(acceptedIDs, ",") != strings.Join(wantIDs, ",") {
		t.Fatalf("accepted order want %v, got %v", wantIDs, acceptedIDs)
	}
	if !sawV2Default {
		t.Fatalf("v2 record should carry bid_size=7")
	}
	// 序号全局连续（accepted 与终态结论共用序号空间）
	for i, o := range items {
		if o.Seq != int64(i+1) {
			t.Fatalf("global seq not contiguous at %d: got %d", i, o.Seq)
		}
	}
	t.Logf("  ✔ accepted=%d 顺序=%v", stats.Accepted, acceptedIDs)
	t.Logf("  ✔ duplicate=%d late_drop=%d schema_rejected=%d，统计各原因可区分",
		stats.Duplicate, stats.DroppedLate, stats.SchemaRejected)
	t.Logf("  ✔ v2 记录 bid_size=7 正确演进；全局序号 1..%d 连续", len(items))
}

// runCrashable 以前 maxLines 行作为输入，EOF 时不冲刷（模拟进程被杀），断点保留 pending。
func runCrashable(t *testing.T, lines []string, maxLines int, sk sink.Sink, cp checkpoint.Store, restore *checkpoint.State, epoch int64) (Stats, *Pipeline) {
	t.Helper()
	input := strings.Join(lines[:maxLines], "\n") + "\n"
	clock := &fakeClock{t: time.Date(2026, 10, 6, 10, 0, 0, 0, time.UTC)}
	logger := log.New(&testWriter{t: t}, "    ", 0)
	cfg := Config{AllowedLateness: 2 * time.Second, BufferLimit: 1000, InputQueue: 4,
		CheckpointEvery: 0, CheckpointTTL: 0, Epoch: epoch, WatermarkInterval: 0}
	cfg.FlushOnEOF = FlushOnEOFOff
	p := New(cfg, testRegistry(t), sk, cp, clock, logger)
	if restore != nil {
		if err := p.Restore(restore); err != nil {
			t.Fatalf("restore: %v", err)
		}
	}
	stats, err := p.Run(context.Background(), ingress.NewReader(strings.NewReader(input)))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	return stats, p
}

// TestIntegration_CrashResumeNoMoreNoLess 崩溃恢复：最终结果不多不少。
func TestIntegration_CrashResumeNoMoreNoLess(t *testing.T) {
	t.Log("【业务覆盖】中断恢复：在冲刷前崩溃（在途记录进断点），同 epoch 恢复后逐条结果与一次跑完一致")
	lines := []string{
		rec1("s1", "X", 100, 1, 0),
		rec1("s3", "X", 102, 1, 2),
		rec1("s2", "X", 101, 1, 1), // 切点：3 条全在 pending，尚未到 watermark
		rec1("s1", "X", 100, 1, 0),
		ctrl(3),
		rec2("s4", "Y", 2000, 2, 4, 7),
		rec1("s5", "X", 105, 1, 5),
		ctrl(6),
		rec1("s6", "X", 106, 1, 6),
		ctrl(8),
	}

	// 基准：一次跑完
	base := &sink.MemorySink{}
	baseStats, _ := runOnce(t, strings.Join(lines, "\n")+"\n", base, &memCP{}, nil, 1000, 1)

	// 第一次运行：在第 3 行后“崩溃”，3 条全部留在 pending 并进入断点
	sk := &sink.MemorySink{}
	cp := &memCP{}
	stats1, _ := runCrashable(t, lines, 3, sk, cp, nil, 1)
	if stats1.Accepted != 0 {
		t.Fatalf("pre-watermark crash should accept 0, got %d", stats1.Accepted)
	}
	saved, err := cp.Load(context.Background())
	if err != nil {
		t.Fatalf("load cp: %v", err)
	}
	if !bytes.Contains(saved.Processor, []byte(`"pending":[`)) {
		t.Fatalf("crash checkpoint must retain pending, processor=%s", string(saved.Processor))
	}
	if saved.InputOffset == 0 {
		t.Fatalf("checkpoint offset should advance past consumed lines")
	}
	t.Logf("  ✔ 崩溃点 accepted=0，checkpoint offset=%d，%d 条在途记录已持久化", saved.InputOffset, 3)

	// 恢复：同 epoch，从断点偏移续跑剩余输入（跳过前 3 行），sink 保留
	rest := lines[3:]
	stats2, _ := runOnce(t, strings.Join(rest, "\n")+"\n", sk, cp, saved, 1000, 1)

	items, baseItems := sk.Items(), base.Items()
	if len(items) != len(baseItems) {
		t.Fatalf("恢复后结果数 want %d（不多不少），got %d", len(baseItems), len(items))
	}
	for i := range items {
		if items[i].Seq != baseItems[i].Seq || items[i].SeqID != baseItems[i].SeqID ||
			items[i].Reason != baseItems[i].Reason {
			t.Fatalf("第 %d 条不一致:\n got %+v\nwant %+v", i, items[i], baseItems[i])
		}
	}
	if stats1.Accepted+stats2.Accepted != baseStats.Accepted {
		t.Fatalf("accepted 合计 want %d, got %d", baseStats.Accepted, stats1.Accepted+stats2.Accepted)
	}
	if stats2.Duplicate != 1 {
		t.Fatalf("恢复后对 s1 的重投仍应判 1 次 duplicate, got %d", stats2.Duplicate)
	}
	t.Logf("  ✔ 恢复后 accepted=%d duplicate=%d；逐条 seq/reason/seq_id 与一次跑完完全一致",
		stats2.Accepted, stats2.Duplicate)
}

// TestIntegration_ResumeWithWrongEpochRejected 错误纪元不得静默恢复。
func TestIntegration_ResumeWithWrongEpochRejected(t *testing.T) {
	t.Log("【业务覆盖】恢复保护：用不同 epoch 恢复必须被校验器拒绝")
	cp := &memCP{}
	now := time.Now()
	wantState := checkpoint.State{Version: 1, Epoch: 1, InputOffset: 10, SinkSeq: 0, UpdatedAt: now}
	if err := cp.Save(context.Background(), wantState); err != nil {
		t.Fatal(err)
	}
	got, _ := cp.Load(context.Background())
	v := checkpoint.NewValidator(0)
	if err := v.Validate(got, 2, 0, now); err == nil {
		t.Fatal("wrong epoch must be rejected")
	} else {
		t.Logf("  ✔ 不同 epoch 恢复被拒绝: %v", err)
	}
}

// TestIntegration_BackpressureBounded 突发流量下内存有界。
func TestIntegration_BackpressureBounded(t *testing.T) {
	t.Log("【业务覆盖】突发流量：buffer-limit 很小时，处理走背压路径但不丢数据、不无限增长")
	var in strings.Builder
	// 1000 条事件时间很远、无 watermark 冲刷，缓冲上限仅 10
	for i := 0; i < 1000; i++ {
		in.WriteString(rec1("b"+itoa(int64(i)), "X", int64(100+i), 1, 100+i) + "\n")
	}
	in.WriteString(ctrl(2000) + "\n")

	sk := &sink.MemorySink{}
	cp := &memCP{}
	stats, _ := runOnce(t, in.String(), sk, cp, nil, 10, 1)

	if stats.Accepted != 1000 {
		t.Fatalf("backpressure path must not lose data: accepted want 1000, got %d", stats.Accepted)
	}
	if stats.Backpressured == 0 {
		t.Fatalf("expected backpressure count > 0 with tiny buffer")
	}
	if len(sk.Items()) != 1000 {
		t.Fatalf("sink items want 1000, got %d", len(sk.Items()))
	}
	t.Logf("  ✔ 1000 条突发 + 缓冲上限 10：backpressured=%d，accepted=%d，零丢失、零重复",
		stats.Backpressured, stats.Accepted)
}
