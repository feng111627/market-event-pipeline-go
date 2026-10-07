package pipeline

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/feng111627/market-event-pipeline-go/internal/checkpoint"
	"github.com/feng111627/market-event-pipeline-go/internal/event"
	"github.com/feng111627/market-event-pipeline-go/internal/schema"
)

var base = time.Date(2026, 10, 6, 10, 0, 0, 0, time.UTC)

// sliceSource 是基于切片的测试数据源。
type sliceSource struct {
	recs []SourceRecord
	pos  int64
}

func (s *sliceSource) Next() (SourceRecord, bool) {
	if int(s.pos) >= len(s.recs) {
		return SourceRecord{}, false
	}
	r := s.recs[s.pos]
	s.pos++
	return r, true
}

func (s *sliceSource) SeekTo(offset int64) error {
	if offset < 0 || offset > int64(len(s.recs)) {
		return fmt.Errorf("seek offset %d out of range", offset)
	}
	s.pos = offset
	return nil
}

// memSink 是幂等的内存 Sink（按事件 ID 去重），记录全部 Emit 调用次数。
type memSink struct {
	seen      map[string]struct{}
	events    []event.MarketEvent
	emitCalls int
	onEmit    func(ev event.MarketEvent) // 注入钩子（模拟崩溃）
}

func newMemSink() *memSink { return &memSink{seen: make(map[string]struct{})} }

func (m *memSink) Emit(ev event.MarketEvent) error {
	m.emitCalls++
	if m.onEmit != nil {
		m.onEmit(ev)
	}
	if _, dup := m.seen[ev.ID]; dup {
		return nil // 幂等：重放不重复计入
	}
	m.seen[ev.ID] = struct{}{}
	m.events = append(m.events, ev)
	return nil
}

func (m *memSink) ids() []string {
	out := make([]string, len(m.events))
	for i, e := range m.events {
		out[i] = e.ID
	}
	return out
}

func rec(offset int64, version int, id string, seq int64, sec int, price float64) SourceRecord {
	return SourceRecord{
		Offset:        offset,
		SchemaVersion: version,
		Payload: map[string]interface{}{
			FieldID: id, FieldSymbol: "BTCUSDT", FieldPrice: price,
			FieldVolume: 1.0, FieldSeq: seq, FieldEventTime: base.Add(time.Duration(sec) * time.Second),
		},
	}
}

func testRegistry(t *testing.T) *schema.Registry {
	t.Helper()
	r := schema.NewRegistry()
	if err := r.Register(MarketEventSchemaV1()); err != nil {
		t.Fatal(err)
	}
	if err := r.Register(MarketEventSchemaV2()); err != nil {
		t.Fatal(err)
	}
	return r
}

func testConfig(t *testing.T) Config {
	t.Helper()
	c := DefaultConfig()
	c.CheckpointPath = filepath.Join(t.TempDir(), "cp.json")
	c.CheckpointEvery = 5
	c.MaxOutOfOrderness = 5 * time.Second
	c.AllowedLateness = 2 * time.Second
	return c
}

// TestOutOfOrderAndDuplicates 覆盖乱序等待、重复丢弃、过期丢弃、更正合并、迟到丢弃。
func TestOutOfOrderAndDuplicates(t *testing.T) {
	t.Log("业务场景: 乱序到达 + 重复投递 + 版本更正 + 迟到事件，每种处置都有明确结论")
	cfg := testConfig(t)
	p := New(cfg, testRegistry(t))
	src := &sliceSource{recs: []SourceRecord{
		rec(0, 1, "e3", 1, 13, 100), // 先到大事件时间
		rec(1, 1, "e1", 1, 11, 100), // 乱序：窗口内，等待
		rec(2, 1, "e1", 1, 11, 100), // 重复投递：丢弃
		rec(3, 1, "e2", 1, 12, 100), // 乱序：窗口内，等待
		rec(4, 1, "e2", 0, 12, 999), // 过期版本：丢弃
		rec(5, 1, "e2", 2, 12, 101), // 更正版本：合并替换缓冲中的 e2
		rec(6, 1, "e4", 1, 20, 100), // 推进水位线到 15，弹出 e1/e2/e3
		rec(7, 1, "e5", 1, 10, 100), // 迟到：事件时间 10 <= 15-2，丢弃
	}}
	sink := newMemSink()
	if err := p.Run(context.Background(), src, sink); err != nil {
		t.Fatalf("Run 失败: %v", err)
	}
	st := p.Stats()
	t.Logf("统计: %+v", st)
	if st.Accepted != 4 || st.Duplicates != 1 || st.Stales != 1 || st.Merged != 1 || st.LateDropped != 1 {
		t.Fatalf("处置统计不符合预期: %+v", st)
	}
	// 输出应为 e1,e2(更正后),e3,e4，且 e2 是更正后的价格。
	want := []string{"e1", "e2", "e3", "e4"}
	got := sink.ids()
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("输出顺序 = %v, 期望 %v", got, want)
	}
	if sink.events[1].Price != 101 {
		t.Fatalf("e2 应为合并后的更正版本 price=101, 得到 %v", sink.events[1].Price)
	}
	// 每种处置都有可区分的回执。
	seen := map[event.Disposition]bool{}
	for _, r := range p.Results() {
		seen[r.Disposition] = true
	}
	for _, d := range []event.Disposition{event.Accepted, event.DuplicateDropped, event.StaleDropped, event.Merged, event.LateDropped} {
		if !seen[d] {
			t.Errorf("缺少处置回执: %v", d)
		}
	}
	t.Logf("用例结果: 输出=%v 统计=%+v，五类处置回执齐全 [通过]", got, st)
}

// TestSchemaEvolution 覆盖结构演进：V2 可选字段缺省填充、未知版本拒绝、不兼容演进拒绝。
func TestSchemaEvolution(t *testing.T) {
	t.Log("业务场景: 上游结构从 V1 演进到 V2（新增可选字段），旧记录用缺省值，未知版本被拒绝")
	cfg := testConfig(t)
	p := New(cfg, testRegistry(t))
	v2rec := rec(0, 2, "e1", 1, 11, 100) // V2 记录，缺 exchange 字段
	badVersion := rec(1, 99, "e2", 1, 12, 100)
	src := &sliceSource{recs: []SourceRecord{v2rec, badVersion, rec(2, 1, "e3", 1, 20, 100)}}
	sink := newMemSink()
	if err := p.Run(context.Background(), src, sink); err != nil {
		t.Fatalf("Run 失败: %v", err)
	}
	st := p.Stats()
	if st.Accepted != 2 || st.Rejected != 1 {
		t.Fatalf("统计不符合预期: %+v", st)
	}
	// V2 缺省值语义（schema 层直接验证）。
	sch, _ := testRegistry(t).Get(2)
	norm, err := sch.Normalize(v2rec.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if norm["exchange"] != "UNKNOWN" {
		t.Fatalf("V2 新增字段缺省值 = %v, 期望 UNKNOWN", norm["exchange"])
	}
	// 不兼容演进必须被拒绝。
	r := testRegistry(t)
	bad := MarketEventSchemaV2()
	bad.Version = 3
	bad.Fields = bad.Fields[:len(bad.Fields)-2] // 删除字段
	if err := r.Register(bad); err == nil {
		t.Fatal("不兼容演进未被拒绝")
	} else {
		t.Logf("不兼容演进被拒绝: %v", err)
	}
	t.Logf("用例结果: V2 记录接受(缺省值 UNKNOWN)，未知版本拒绝，不兼容演进拒绝 [通过] stats=%+v", st)
}

// TestCrashRecovery 覆盖中断恢复：崩溃后从断点继续，最终结果不多也不少。
func TestCrashRecovery(t *testing.T) {
	t.Log("业务场景: 处理中途崩溃，重启后从断点恢复，最终输出与无崩溃运行完全一致")
	cfg := testConfig(t)
	cfg.CheckpointEvery = 3

	const total = 20
	var recs []SourceRecord
	for i := 0; i < total; i++ {
		recs = append(recs, rec(int64(i), 1, fmt.Sprintf("e%02d", i), 1, i, 100))
	}

	// 第一次运行：输出到第 9 条时模拟崩溃。
	// 崩溃点位于两次断点提交之间，恢复后已输出但未入断点的事件会被重放，
	// 由幂等 Sink 吸收，验证"结果不多也不少"。
	ctx, cancel := context.WithCancel(context.Background())
	sink := newMemSink()
	var emitted int64
	sink.onEmit = func(ev event.MarketEvent) {
		if atomic.AddInt64(&emitted, 1) == 9 {
			cancel() // 模拟崩溃
		}
	}
	p1 := New(cfg, testRegistry(t))
	err := p1.Run(ctx, &sliceSource{recs: recs}, sink)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("应因取消而中断, 得到 %v", err)
	}
	t.Logf("第一次运行中断: 已输出 %d 条, 断点=%+v", len(sink.events), mustLoad(t, cfg.CheckpointPath).Offset)

	// 第二次运行：新进程（新 Pipeline），从断点恢复，复用同一 Sink（幂等）。
	p2 := New(cfg, testRegistry(t))
	if err := p2.Resume(context.Background(), &sliceSource{recs: recs}, sink, -1); err != nil {
		t.Fatalf("Resume 失败: %v", err)
	}

	if len(sink.events) != total {
		t.Fatalf("恢复后唯一输出 = %d, 期望 %d（事件丢失或重复计入）", len(sink.events), total)
	}
	if sink.emitCalls < len(sink.events) {
		t.Fatalf("Emit 调用次数 %d 小于唯一输出 %d，异常", sink.emitCalls, len(sink.events))
	}
	for i, ev := range sink.events {
		if ev.ID != fmt.Sprintf("e%02d", i) {
			t.Fatalf("第 %d 条输出 = %s, 期望 e%02d（顺序错乱）", i, ev.ID, i)
		}
	}
	t.Logf("用例结果: 恢复后输出完整 %d 条且顺序正确；Sink 幂等吸收了 %d 次重放 [通过]",
		len(sink.events), sink.emitCalls-len(sink.events))
}

// TestExpiredCheckpointRejected 验证过期断点被拒绝。
func TestExpiredCheckpointRejected(t *testing.T) {
	t.Log("业务场景: 断点已过期，恢复必须被拒绝而不是从中间接着算")
	cfg := testConfig(t)
	cfg.CheckpointTTL = time.Hour
	// 手工写入一个 2 小时前提交的断点。
	store := checkpoint.NewStore(cfg.CheckpointPath, cfg.CheckpointTTL)
	if err := store.Save(checkpoint.State{
		Offset: 5, Committed: time.Now().Add(-2 * time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	p := New(cfg, testRegistry(t))
	err := p.Resume(context.Background(), &sliceSource{}, newMemSink(), -1)
	if !errors.Is(err, checkpoint.ErrExpired) {
		t.Fatalf("应返回 ErrExpired, 得到 %v", err)
	}
	t.Logf("用例结果: 过期断点被拒绝 err=%v [通过]", err)
}

// TestRollbackRejected 验证位点回退被拒绝。
func TestRollbackRejected(t *testing.T) {
	t.Log("业务场景: 请求从比已提交位点更早的位置恢复，必须明确拒绝")
	cfg := testConfig(t)
	p1 := New(cfg, testRegistry(t))
	src := &sliceSource{recs: []SourceRecord{rec(0, 1, "e1", 1, 1, 100)}}
	if err := p1.Run(context.Background(), src, newMemSink()); err != nil {
		t.Fatal(err)
	}
	committed := mustLoad(t, cfg.CheckpointPath).Offset
	p2 := New(cfg, testRegistry(t))
	err := p2.Resume(context.Background(), &sliceSource{recs: src.recs}, newMemSink(), committed-1)
	if !errors.Is(err, checkpoint.ErrRollback) {
		t.Fatalf("应返回 ErrRollback, 得到 %v", err)
	}
	t.Logf("用例结果: 回退到 %d（已提交 %d）被拒绝 err=%v [通过]", committed-1, committed, err)
}

// TestBurstBackpressure 验证突发流量下有界缓冲与恒定内存。
func TestBurstBackpressure(t *testing.T) {
	t.Log("业务场景: 突发流量（5 万条）涌入，输入通道容量有界，流水线稳定消化不撑爆内存")
	cfg := testConfig(t)
	cfg.InputBufferSize = 8 // 极小缓冲，放大背压效应
	cfg.CheckpointEvery = 1000
	cfg.ResultRingSize = 100

	const total = 50000
	recs := make([]SourceRecord, total)
	for i := 0; i < total; i++ {
		recs[i] = rec(int64(i), 1, fmt.Sprintf("e%d", i), 1, i, 100)
	}
	p := New(cfg, testRegistry(t))
	sink := newMemSink()
	start := time.Now()
	if err := p.Run(context.Background(), &sliceSource{recs: recs}, sink); err != nil {
		t.Fatalf("Run 失败: %v", err)
	}
	st := p.Stats()
	if st.Accepted != total || st.Emitted != total || len(sink.events) != total {
		t.Fatalf("突发流量处理不完整: %+v, sink=%d", st, len(sink.events))
	}
	if len(p.Results()) > 100 {
		t.Fatalf("回执环形缓冲超出容量: %d", len(p.Results()))
	}
	t.Logf("用例结果: %d 条全部处理，耗时 %v，断点提交 %d 次，回执缓冲恒定 %d 条 [通过]",
		total, time.Since(start).Round(time.Millisecond), st.Checkpoints, len(p.Results()))
}

func mustLoad(t *testing.T, path string) *checkpoint.State {
	t.Helper()
	st, err := checkpoint.NewStore(path, 0).Load()
	if err != nil || st == nil {
		t.Fatalf("加载断点失败: %v", err)
	}
	return st
}

// recV3 构造带 V3 扩展字段的记录。
func recV3(offset int64, id string, seq int64, sec int, attrs map[string]interface{}) SourceRecord {
	payload := map[string]interface{}{
		FieldID: id, FieldSymbol: "BTCUSDT", FieldPrice: 100.0,
		FieldVolume: 1.0, FieldSeq: seq, FieldEventTime: base.Add(time.Duration(sec) * time.Second),
	}
	for k, v := range attrs {
		payload[k] = v
	}
	return SourceRecord{Offset: offset, SchemaVersion: 3, Payload: payload}
}

// TestAttrsSurviveCheckpoint 验证扩展字段与结构版本随缓冲快照进断点、
// 恢复后输出到 sink 的事件仍带全部字段与正确版本（透传不被断点截断）。
// TestAttrsSurviveCheckpoint 验证扩展字段与结构版本随缓冲快照进断点、
// 恢复后输出到 sink 的事件仍带全部字段与正确版本（透传不被断点截断）。
func TestAttrsSurviveCheckpoint(t *testing.T) {
	t.Log("业务场景: V3 事件滞留在乱序缓冲中入断点，崩溃恢复后下游仍拿到全部新字段与版本")
	cfg := testConfig(t)
	cfg.CheckpointEvery = 1 // 第一条处理完即提交断点（事件仍在缓冲内）
	cfg.MaxOutOfOrderness = time.Hour
	cfg.AllowedLateness = time.Hour
	recs := []SourceRecord{
		recV3(0, "e1", 1, 5, map[string]interface{}{"is_maker": true, "venue_id": int64(7)}),
		recV3(1, "e2", 1, 6, nil),
	}

	// 第一次运行：第一条处理完并提交断点后，源在第二条处阻塞；测试在确认
	// 断点已落盘后再取消 ctx，确定性模拟"已提交、未收尾"的崩溃。
	p1 := New(cfg, testRegistryV3(t))
	ctx, cancel := context.WithCancel(context.Background())
	sink1 := newMemSink()
	committed := make(chan struct{})
	src1 := &blockingAfterFirstSource{recs: recs, onBlock: func() {
		waitFor(t, func() bool { return p1.Stats().Checkpoints >= 1 })
		close(committed)
	}}
	go func() {
		<-committed
		cancel()
	}()
	if err := p1.Run(ctx, src1, sink1); !errors.Is(err, context.Canceled) {
		t.Fatalf("第一次运行应被取消, 得到 %v", err)
	}

	cp := mustLoad(t, cfg.CheckpointPath)
	if len(cp.Buffered) != 1 {
		t.Fatalf("断点缓冲应含 1 条（大窗口下未提前输出）, 得到 %d", len(cp.Buffered))
	}
	if cp.Buffered[0].SchemaVersion != 3 {
		t.Fatalf("缓冲快照版本=%d, 期望 3", cp.Buffered[0].SchemaVersion)
	}
	if cp.Buffered[0].Attrs["is_maker"] != true || cp.Buffered[0].Attrs["venue_id"] != int64(7) {
		t.Fatalf("缓冲快照扩展字段丢失: %+v", cp.Buffered[0].Attrs)
	}
	if cp.Buffered[0].Attrs["exchange"] != "UNKNOWN" {
		t.Fatalf("缺省扩展字段 exchange 未进快照: %+v", cp.Buffered[0].Attrs)
	}

	// 用全新 pipeline 从断点恢复并跑完（幂等 sink）。
	p2 := New(cfg, testRegistryV3(t))
	sink2 := newMemSink()
	if err := p2.Resume(context.Background(), &sliceSource{recs: recs}, sink2, -1); err != nil {
		t.Fatalf("Resume 失败: %v", err)
	}
	if len(sink2.events) != 2 {
		t.Fatalf("恢复后应输出 2 条, 得到 %d", len(sink2.events))
	}
	got := sink2.events[0]
	if got.SchemaVersion() != 3 || got.Attrs["is_maker"] != true ||
		got.Attrs["venue_id"] != int64(7) || got.Attrs["exchange"] != "UNKNOWN" {
		t.Fatalf("恢复后扩展字段/版本不完整: version=%d attrs=%+v", got.SchemaVersion(), got.Attrs)
	}
	t.Logf("用例结果: V3 事件经断点缓冲快照恢复，版本与全部扩展字段（含缺省）完整到达 sink [通过] attrs=%+v", got.Attrs)
}
func TestBoundedBufferForcesAndDropsLate(t *testing.T) {
	t.Log("业务场景: 乱序持续超过缓冲上界，触发强制输出并对超地板的更旧事件判迟到")
	cfg := testConfig(t)
	cfg.MaxOutOfOrderness = time.Hour // 排除常规水位线迟到
	cfg.AllowedLateness = 0
	cfg.MaxBufferSize = 3
	var recs []SourceRecord
	// 逆序投递 6 个事件时间唯一的事件。
	for i := 0; i < 6; i++ {
		recs = append(recs, rec(int64(i), 1, fmt.Sprintf("e%02d", i), 1, 9-i, 100))
	}
	p := New(cfg, testRegistry(t))
	sink := newMemSink()
	if err := p.Run(context.Background(), &sliceSource{recs: recs}, sink); err != nil {
		t.Fatalf("Run 失败: %v", err)
	}
	st := p.Stats()
	if st.BufferedMax > 3 {
		t.Fatalf("缓冲峰值=%d 超过上界 3", st.BufferedMax)
	}
	if st.ForcedEmitted < 1 {
		t.Fatal("应触发强制输出")
	}
	// 输出（含强制 + 收尾 flush）必须按事件时间非降序。
	var prev time.Time
	for i, ev := range sink.events {
		if i > 0 && ev.EventTime.Before(prev) {
			t.Fatalf("输出失去事件时间顺序: %s < %s", ev.EventTime, prev)
		}
		prev = ev.EventTime
	}
	t.Logf("用例结果: 缓冲峰值=%d、强制输出=%d、输出 %d 条有序，统计=%+v [通过]",
		st.BufferedMax, st.ForcedEmitted, len(sink.events), st)
}

// blockingAfterFirstSource 返回第一条后，在第二次 Next 回调 onBlock 并阻塞，
// 用于在"第一条已处理且提交断点"后确定性地注入取消。
type blockingAfterFirstSource struct {
	recs    []SourceRecord
	pos     int64
	onBlock func()
}

func (s *blockingAfterFirstSource) Next() (SourceRecord, bool) {
	if s.pos == 1 {
		if s.onBlock != nil {
			s.onBlock()
		}
		time.Sleep(200 * time.Millisecond) // 取消在这段时间内发生
		return SourceRecord{}, false
	}
	if int(s.pos) >= len(s.recs) {
		return SourceRecord{}, false
	}
	r := s.recs[s.pos]
	s.pos++
	return r, true
}

func (s *blockingAfterFirstSource) SeekTo(offset int64) error { s.pos = offset; return nil }

// waitFor 轮询条件成立，超时即失败。
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("等待条件超时")
}

func testRegistryV3(t *testing.T) *schema.Registry {
	t.Helper()
	r := testRegistry(t)
	if err := r.Register(MarketEventSchemaV3()); err != nil {
		t.Fatal(err)
	}
	return r
}
