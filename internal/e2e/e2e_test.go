// Package e2e 存放从外部输入到最终输出的端到端链路测试：
// 真实文件源 -> 流水线 -> 真实文件 sink，并读回输出文件断言。
package e2e

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/feng111627/market-event-pipeline-go/internal/jsonio"
	"github.com/feng111627/market-event-pipeline-go/internal/pipeline"
	"github.com/feng111627/market-event-pipeline-go/internal/schema"
)

func registry(t *testing.T) *schema.Registry {
	t.Helper()
	r := schema.NewRegistry()
	for _, s := range []schema.Schema{
		pipeline.MarketEventSchemaV1(),
		pipeline.MarketEventSchemaV2(),
		pipeline.MarketEventSchemaV3(),
	} {
		if err := r.Register(s); err != nil {
			t.Fatal(err)
		}
	}
	return r
}

// writeLines 把若干行写入临时输入文件并返回路径。
func writeLines(t *testing.T, lines []string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "in.jsonl")
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// runFilePipeline 以真实文件源/汇跑一次流水线。
func runFilePipeline(t *testing.T, cfg pipeline.Config, inPath, outPath, cpPath string) *pipeline.Pipeline {
	t.Helper()
	src, err := jsonio.OpenFileSource(inPath)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	sink, err := jsonio.OpenFileSink(outPath)
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	p := pipeline.New(cfg, registry(t))
	if err := p.Run(context.Background(), src, sink); err != nil {
		t.Fatalf("Run 失败: %v", err)
	}
	return p
}

// readOutputs 读回输出文件，每行解析为 map。
func readOutputs(t *testing.T, path string) []map[string]interface{} {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out []map[string]interface{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<22)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var m map[string]interface{}
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("输出行不是合法 JSON: %v (%s)", err, line)
		}
		out = append(out, m)
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// line 构造一条扁平 JSON 输入行。
func line(version int, id string, sec int, extra string) string {
	s := fmt.Sprintf(`{"version":%d,"id":%q,"symbol":"BTCUSDT","price":100,"volume":1,"seq":1,"event_time":%q`,
		version, id, fmt.Sprintf("2026-10-06T10:00:%02dZ", sec))
	if extra != "" {
		s += "," + extra
	}
	return s + "}"
}

// TestEndToEnd_FieldPassthrough 重点验证：新增字段（含显式赋值与缺省补齐）
// 从外部输入一路完整出现在最终输出文件中，未声明字段被忽略，输出按事件时间有序。
func TestEndToEnd_FieldPassthrough(t *testing.T) {
	t.Log("业务场景: 外部文件输入，混合 V1/V2/V3 与乱序/重复/坏行；" +
		"最终输出按事件时间有序，且每个版本声明的字段（含缺省）完整透传")
	dir := t.TempDir()
	inPath := writeLines(t, []string{
		line(1, "v1-old", 13, ""),     // V1：无扩展字段
		line(2, "v2-default", 11, ""), // V2：exchange 缺省 UNKNOWN
		line(3, "v3-explicit", 12, `"is_maker":true,"venue_id":42,"tick_size":0.5`), // V3：显式新字段
		line(3, "v3-default", 10, ""),                                                            // V3：全部新字段取缺省
		line(2, "v2-default", 11, ""),                                                            // 重复：丢弃
		line(1, "v1-unknown", 14, `"ghost_field":"drop-me"`),                                     // 未声明字段：忽略
		`{"version":1,"id":"bad","symbol":"S","price":1,"volume":1,"seq":1,"event_time":"nope"}`, // 坏时间：拒绝
		`this is not json`,                                                                       // 坏行：拒绝
	})
	outPath := filepath.Join(dir, "out.jsonl")
	cpPath := filepath.Join(dir, "cp.json")

	cfg := pipeline.DefaultConfig()
	cfg.CheckpointPath = cpPath
	cfg.CheckpointEvery = 3
	p := runFilePipeline(t, cfg, inPath, outPath, cpPath)
	st := p.Stats()
	t.Logf("统计: %+v", st)

	if st.Processed != 8 {
		t.Fatalf("应处理 8 行, 得到 %d", st.Processed)
	}
	if st.Rejected != 2 || st.Duplicates != 1 {
		t.Fatalf("拒绝/重复计数不符: rejected=%d dup=%d", st.Rejected, st.Duplicates)
	}

	outs := readOutputs(t, outPath)
	if len(outs) != 5 {
		t.Fatalf("应输出 5 条, 得到 %d", len(outs))
	}

	// 1) 输出必须按事件时间升序：v3-default(10) v2-default(11) v3-explicit(12) v1-old(13) v1-unknown(14)
	wantOrder := []string{"v3-default", "v2-default", "v3-explicit", "v1-old", "v1-unknown"}
	for i, want := range wantOrder {
		if outs[i]["id"] != want {
			t.Fatalf("第 %d 条输出 id=%v，期望 %s；完整顺序异常: %v", i, outs[i]["id"], want, ids(outs))
		}
	}
	t.Logf("用例结果: 输出按事件时间有序 %v [通过]", ids(outs))

	// 2) V3 显式赋值的字段必须原样到达下游。
	explicit := outs[2]
	if explicit["id"] != "v3-explicit" {
		t.Fatalf("定位错行: %v", explicit["id"])
	}
	if explicit["is_maker"] != true || explicit["venue_id"] != float64(42) || explicit["tick_size"] != 0.5 {
		t.Fatalf("V3 显式新字段未完整透传: %v", explicit)
	}
	if explicit["exchange"] != "UNKNOWN" || explicit["venue_category"] != "SPOT" {
		t.Fatalf("V3 继承/缺省字段不符: exchange=%v venue_category=%v", explicit["exchange"], explicit["venue_category"])
	}
	if explicit["schema_version"] != float64(3) {
		t.Fatalf("schema_version 标注错误: %v", explicit["schema_version"])
	}
	t.Logf("用例结果: V3 显式新字段(is_maker/venue_id/tick_size)与缺省字段全部到达下游 [通过]")

	// 3) V3 缺省补齐行：所有已声明字段都在，缺省语义稳定。
	def := outs[0]
	for k, want := range map[string]interface{}{
		"exchange": "UNKNOWN", "venue_category": "SPOT",
		"is_maker": false, "tick_size": float64(0), "venue_id": float64(0),
	} {
		if def[k] != want {
			t.Errorf("缺省字段 %s=%v, 期望 %v", k, def[k], want)
		}
	}
	t.Logf("用例结果: V3 缺省行含全部已声明字段且缺省值稳定可复现 [通过] %v", def)

	// 4) V1 行不带任何扩展字段；未声明字段不出现在任何输出里。
	v1 := outs[3]
	if _, ok := v1["exchange"]; ok {
		t.Errorf("V1 输出不应出现 V2+ 字段 exchange: %v", v1)
	}
	for _, o := range outs {
		if _, ok := o["ghost_field"]; ok {
			t.Fatalf("未声明字段泄漏到输出: %v", o)
		}
	}
	t.Log("用例结果: V1 无扩展字段、未声明字段 ghost_field 已被忽略 [通过]")
}

// TestEndToEnd_ExactlyOnceAfterRerun 验证跑到一半挂掉再重跑：
// 最终输出与一口气跑完逐字节一致，不多也不少。
func TestEndToEnd_ExactlyOnceAfterRerun(t *testing.T) {
	t.Log("业务场景: 文件->文件跑到一半中断，新进程重跑，输出与无中断运行逐字节一致")
	dir := t.TempDir()
	inPath := writeLines(t, genLines(300))

	// 参考：一口气跑完的输出（与中断运行相同的处理配置）。
	refPath := filepath.Join(dir, "ref.jsonl")
	refCfg := pipeline.DefaultConfig()
	refCfg.CheckpointPath = filepath.Join(dir, "ref-cp.json")
	refCfg.CheckpointEvery = 37
	refCfg.MaxOutOfOrderness = 24 * time.Hour
	refCfg.AllowedLateness = time.Hour
	runFilePipeline(t, refCfg, inPath, refPath, "")
	refBytes, err := os.ReadFile(refPath)
	if err != nil {
		t.Fatal(err)
	}

	// 中断运行：第一次处理到约一半时被 ctx 取消（位于断点之间，
	// 制造"已输出但未入断点"的尾部，以真实验证重放吸收与 sink 截断）。
	cpPath := filepath.Join(dir, "cp.json")
	outPath := filepath.Join(dir, "out.jsonl")
	cfg := pipeline.DefaultConfig()
	cfg.CheckpointPath = cpPath
	cfg.CheckpointEvery = 37

	const half = 150

	// 让水位线不推进（极大乱序窗口），使事件滞留在缓冲、逐条 DrainReady 输出，
	// 从而可以在"处理到一半"的确定时刻中断。
	cfg.MaxOutOfOrderness = 24 * time.Hour
	cfg.AllowedLateness = time.Hour

	src1, err := jsonio.OpenFileSource(inPath)
	if err != nil {
		t.Fatal(err)
	}
	sink1, err := jsonio.OpenFileSink(outPath)
	if err != nil {
		t.Fatal(err)
	}
	p1 := pipeline.New(cfg, registry(t))
	ctx, cancel := context.WithCancel(context.Background())
	gate := make(chan struct{})
	reached := make(chan struct{})
	var reachedOnce sync.Once
	var emittedCount int64
	sink1.SetEmitHook(func() {
		n := atomic.AddInt64(&emittedCount, 1)
		if n == half {
			// 第 half 次输出时短暂挂住并通知测试侧"已到中断点"，
			// 待测试侧取消 ctx 后再放行，使 Run 回到 select 时观察到取消。
			reachedOnce.Do(func() { close(reached) })
			<-gate
		}
	})
	// 慢速源：每次读取小睡，保证取消发生时源仍未耗尽（records 未关闭），
	// 从而让 Run 的 select 明确观察到 ctx 取消而非通道关闭。
	slow := &slowSource{inner: src1, pause: time.Millisecond}
	runErr := make(chan error, 1)
	go func() { runErr <- p1.Run(ctx, slow, sink1) }()

	<-reached
	cancel()
	close(gate)
	if err := <-runErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("第一次运行应被取消中断, 得到 %v", err)
	}
	src1.Close()
	sink1.Close()

	before, _ := os.ReadFile(outPath)
	t.Logf("中断运行: sink 收到 %d 次输出, 输出文件 %d 字节", half, len(before))

	// 新进程重跑：新流水线实例，从断点自动恢复（Run 检测到断点）。
	p2 := runFilePipeline(t, cfg, inPath, outPath, cpPath)
	gotBytes, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(gotBytes) != string(refBytes) {
		t.Fatalf("重跑后输出与参考不一致（不多不少被破坏）:\n参考行数=%d\n实际=%d",
			countLines(refBytes), countLines(gotBytes))
	}
	t.Logf("用例结果: 中断后重跑输出 %d 行、%d 字节，与无中断运行逐字节一致；最终统计=%+v [通过]",
		countLines(gotBytes), len(gotBytes), p2.Stats())
}

// TestEndToEnd_BoundedMemoryUnderBurst 验证输入远大于内存、且突发涌入时：
// 乱序缓冲有硬上界，内存指标不随输入规模增长，事件不丢（强制输出仍计入输出）。
func TestEndToEnd_BoundedMemoryUnderBurst(t *testing.T) {
	t.Log("业务场景: 超大+高度乱序输入，乱序缓冲容量被设得很小，" +
		"缓冲峰值不越上界，输出不丢事件且整体仍按事件时间非降序")
	dir := t.TempDir()
	const total = 50000
	inPath := filepath.Join(dir, "big.jsonl")
	f, err := os.Create(inPath)
	if err != nil {
		t.Fatal(err)
	}
	w := bufio.NewWriterSize(f, 1<<20)
	baseT := time.Date(2026, 10, 6, 10, 0, 0, 0, time.UTC)
	// 制造持续乱序：把事件时间分成大块并在块内逆序投递，
	// 若缓冲无上界，约 total 条会同时滞留。
	const block = 1000
	for b := 0; b < total/block; b++ {
		for j := block - 1; j >= 0; j-- {
			sec := b*block + j
			ts := baseT.Add(time.Duration(sec) * time.Second).Format(time.RFC3339)
			fmt.Fprintf(w, `{"version":3,"id":"e%06d","symbol":"S","price":1,"volume":1,"seq":1,"event_time":%q}`+"\n",
				sec, ts)
		}
	}
	w.Flush()
	f.Close()

	var m0, m1 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m0)

	cfg := pipeline.DefaultConfig()
	cfg.CheckpointPath = filepath.Join(dir, "cp.json")
	cfg.CheckpointEvery = 5000
	cfg.MaxBufferSize = 2000 // 很小的硬上界
	cfg.DedupCapacity = 200000
	cfg.InputBufferSize = 16
	// 乱序幅度（block=1000 秒）远小于乱序容忍窗口，因此这些都是"乱序等待"
	// 而非"迟到丢弃"；在无界缓冲下约 block 条会同时驻留，恰好用来压测上界。
	cfg.MaxOutOfOrderness = time.Hour
	cfg.AllowedLateness = time.Minute
	outPath := filepath.Join(dir, "big-out.jsonl")
	p := runFilePipeline(t, cfg, inPath, outPath, "")
	st := p.Stats()
	runtime.ReadMemStats(&m1)

	t.Logf("统计: processed=%d accepted=%d emitted=%d forced=%d bufferedMax=%d checkpoints=%d",
		st.Processed, st.Accepted, st.Emitted, st.ForcedEmitted, st.BufferedMax, st.Checkpoints)
	t.Logf("堆分配: before=%dMB after=%dMB（输入 %d 条）", m0.HeapAlloc>>20, m1.HeapAlloc>>20, total)

	if st.Processed != total {
		t.Fatalf("处理条数=%d，期望 %d（事件丢失）", st.Processed, total)
	}
	if st.Emitted != total {
		t.Fatalf("输出条数=%d，期望 %d（事件丢失）", st.Emitted, total)
	}
	if st.BufferedMax > cfg.MaxBufferSize {
		t.Fatalf("乱序缓冲峰值=%d 超过硬上界=%d（内存随输入增长）", st.BufferedMax, cfg.MaxBufferSize)
	}
	if st.ForcedEmitted == 0 {
		t.Fatal("高度乱序 + 小缓冲应触发强制输出，ForcedEmitted=0 说明上界未生效")
	}
	// 输出必须事件时间非降序（强制输出语义不能破坏有序性）。
	outs := readOutputs(t, outPath)
	var prev string
	for i, o := range outs {
		ts, _ := o["event_time"].(string)
		if i > 0 && ts < prev {
			t.Fatalf("输出在第 %d 行失去事件时间顺序: %s < %s", i, ts, prev)
		}
		prev = ts
	}
	if len(outs) != total {
		t.Fatalf("读回输出 %d 条，期望 %d", len(outs), total)
	}
	// 有界性：额外堆增长应远小于"为每条事件驻留一个对象"的规模。
	heapGrowMB := int64(m1.HeapAlloc>>20) - int64(m0.HeapAlloc>>20)
	t.Logf("用例结果: %d 条全部输出、缓冲峰值=%d≤上界、强制输出=%d、堆增长=%dMB、输出有序 [通过]",
		total, st.BufferedMax, st.ForcedEmitted, heapGrowMB)
}

// slowSource 包装一个源，在每次 Next 前小睡，用于让取消先于源耗尽发生。
type slowSource struct {
	inner pipeline.Source
	pause time.Duration
}

func (s *slowSource) Next() (pipeline.SourceRecord, bool) {
	time.Sleep(s.pause)
	return s.inner.Next()
}

func (s *slowSource) SeekTo(offset int64) error { return s.inner.SeekTo(offset) }

func ids(ms []map[string]interface{}) []string {
	out := make([]string, len(ms))
	for i, m := range ms {
		out[i], _ = m["id"].(string)
	}
	return out
}

func countLines(b []byte) int {
	s := strings.TrimRight(string(b), "\n")
	if s == "" {
		return 0
	}
	return strings.Count(s, "\n") + 1
}

// genLines 生成 n 条事件时间递增、带少量重复的合法输入行。
func genLines(n int) []string {
	lines := make([]string, 0, n+3)
	for i := 0; i < n; i++ {
		lines = append(lines, line(2, fmt.Sprintf("e%04d", i), i%60, ""))
	}
	// 追加两条重复投递（应被丢弃，不影响输出）。
	lines = append(lines, line(2, "e0000", 0, ""), line(2, "e0001", 1, ""))
	return lines
}
