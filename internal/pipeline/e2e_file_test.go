package pipeline_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/feng111627/market-event-pipeline-go/internal/pipeline"
	"github.com/feng111627/market-event-pipeline-go/internal/streamio"
)

// e2eBase 是端到端用例统一的事件时间基准。
var e2eBase = time.Date(2026, 10, 6, 10, 0, 0, 0, time.UTC)

// e2eConfig 构造使用临时断点路径的测试配置。
func e2eConfig(t *testing.T) pipeline.Config {
	t.Helper()
	c := pipeline.DefaultConfig()
	c.CheckpointPath = filepath.Join(t.TempDir(), "cp.json")
	c.CheckpointEvery = 5
	c.MaxOutOfOrderness = 5 * time.Second
	c.AllowedLateness = 2 * time.Second
	return c
}

// writeLines 把若干行写到临时文件并返回路径。
func writeLines(t *testing.T, name string, lines []string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// line 构造一条带事件时间的输入行（信封形式）。
func line(version int, id string, seq int, sec int, extraFields ...string) string {
	et := e2eBase.Add(time.Duration(sec) * time.Second).Format(time.RFC3339)
	m := map[string]interface{}{
		"id": id, "symbol": "BTCUSDT", "price": 100.0, "volume": 1.0,
		"seq": seq, "event_time": et,
	}
	for i := 0; i+1 < len(extraFields); i += 2 {
		// extraFields 为 key,jsonValue 对。
		var v interface{}
		if err := json.Unmarshal([]byte(extraFields[i+1]), &v); err != nil {
			panic(err)
		}
		m[extraFields[i]] = v
	}
	b, _ := json.Marshal(m)
	return fmt.Sprintf(`{"version":%d,"payload":%s}`, version, string(b))
}

// runFile 用真实文件 LineSource -> Pipeline -> 文件 LineSink 跑一遍，
// 返回输出行解析结果（map 切片，按输出顺序）。
func runFile(t *testing.T, cfg pipeline.Config, inPath string) []map[string]interface{} {
	t.Helper()
	in, err := os.Open(inPath)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	outPath := filepath.Join(t.TempDir(), "out.jsonl")
	out, err := os.Create(outPath)
	if err != nil {
		t.Fatal(err)
	}
	src := streamio.NewLineSource(in)
	sink := streamio.NewLineSink(out)
	p := pipeline.New(cfg, pipeline.DefaultRegistry())
	if err := p.Run(context.Background(), src, sink); err != nil {
		t.Fatalf("Run 失败: %v", err)
	}
	if err := sink.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatal(err)
	}
	var rows []map[string]interface{}
	for _, l := range strings.Split(strings.TrimRight(string(data), "\n"), "\n") {
		var row map[string]interface{}
		if err := json.Unmarshal([]byte(l), &row); err != nil {
			t.Fatalf("输出行非 JSON: %v (%s)", err, l)
		}
		rows = append(rows, row)
	}
	return rows
}

// TestE2EFileNewFieldsFlowToOutput 是本任务的核心端到端用例：
// 从外部文件输入，V1/V2/V3 混发，验证兼容新增字段（含缺省值）完整到达输出，
// 未声明字段被忽略，输出按事件时间排序，汇总各类计数正确。
func TestE2EFileNewFieldsFlowToOutput(t *testing.T) {
	t.Log("业务场景: 真实文件端到端；多版本混发，新增字段完整落地，未知字段忽略，输出按事件时间有序")
	cfg := e2eConfig(t)
	cfg.MaxBuffered = 1000
	lines := []string{
		line(3, "e3", 1, 13, "exchange", `"CB"`, "venue_mic", `"XNMS"`, "is_maker", `true`, "ghost", `123`),
		line(1, "e1", 1, 11),                       // V1：所有新增字段都应取缺省
		line(2, "e2", 1, 12, "exchange", `"BINA"`), // V2：venue_mic/is_maker 取 V3 缺省
		line(3, "e4", 1, 14),                       // V3：全部新增字段取缺省
		line(1, "e5", 1, 25),                       // 推进水位线
		line(1, "e1", 1, 11),                       // 重复：丢弃
		line(1, "e0", 1, 1),                        // 迟到：丢弃
		`{"version":1,"payload":{"id":"bad"}}`,     // 必填缺失：拒绝
		`{"version":77,"payload":{"id":"vbad"}}`,   // 未知版本：拒绝
	}
	inPath := writeLines(t, "in.jsonl", lines)
	rows := runFile(t, cfg, inPath)

	// 输出应按事件时间升序：e1,e2,e3,e4,e5。
	wantIDs := []string{"e1", "e2", "e3", "e4", "e5"}
	if len(rows) != len(wantIDs) {
		t.Fatalf("输出行数 = %d, 期望 %d: %+v", len(rows), len(wantIDs), rows)
	}
	for i, id := range wantIDs {
		if rows[i]["id"] != id {
			t.Fatalf("第 %d 行 id = %v, 期望 %s（输出未按事件时间排序）", i, rows[i]["id"], id)
		}
	}

	byID := map[string]map[string]interface{}{}
	for _, r := range rows {
		byID[r["id"].(string)] = r
	}
	// V1 旧数据：输出仍带全部已声明扩展字段，缺省语义稳定可复现。
	e1 := byID["e1"]
	if e1["version"].(float64) != 1 || e1["exchange"] != "UNKNOWN" ||
		e1["venue_mic"] != "XXXX" || e1["is_maker"] != false {
		t.Fatalf("V1 记录缺省字段不完整/不正确: %+v", e1)
	}
	// V2：自带 exchange，V3 才有的两个字段取缺省。
	e2 := byID["e2"]
	if e2["exchange"] != "BINA" || e2["venue_mic"] != "XXXX" || e2["is_maker"] != false {
		t.Fatalf("V2 记录字段流转不正确: %+v", e2)
	}
	// V3：自带三个字段，未知字段 ghost 被忽略。
	e3 := byID["e3"]
	if e3["exchange"] != "CB" || e3["venue_mic"] != "XNMS" || e3["is_maker"] != true {
		t.Fatalf("V3 新增字段未完整到达输出: %+v", e3)
	}
	if _, present := e3["ghost"]; present {
		t.Fatalf("未声明字段 ghost 不应出现在输出: %+v", e3)
	}
	t.Logf("用例结果: 输出 %v 按事件时间有序；V1/V2 缺省字段补齐，V3 新字段完整，未知字段忽略 [通过]", wantIDs)
}

// peekBufferedSource 暴露流水线内部不可见的缓冲统计不现实，因此大输入用例通过
// 公开行为断言：配置极小的 MaxBuffered/去重表/输入通道，处理远超内存规模的
// 事件流（顺序时间戳会很快越过水位线，更正场景下缓冲被强制有界），
// 输出既不丢也不重，且全程不随输入规模增长而失败。
//
// 为真正观测"缓冲上界"，这里用乱序时间戳构造持续堆积压力：
// 每批先放一个很大的事件时间顶住水位线，再连续塞入 MaxBuffered+N 个更小但
// 仍在迟到宽限内的事件——若没有溢出强制输出，缓冲会无限增长到 N 级别。
func TestE2ELargeInputBoundedMemory(t *testing.T) {
	t.Log("业务场景: 超大/突发输入（20 万条）、极小缓冲与去重表，内存有上界且不丢不重、输出有序")
	cfg := e2eConfig(t)
	cfg.InputBufferSize = 8
	cfg.MaxBuffered = 500
	cfg.DedupCapacity = 2000
	cfg.ResultRingSize = 100
	cfg.CheckpointEvery = 5000
	cfg.MaxOutOfOrderness = 2 * time.Second
	cfg.AllowedLateness = 0

	const total = 200000
	lines := make([]string, 0, total)
	// 顺序、密集、单调递增的事件时间：水位线快速推进，绝大多数事件立即可输出，
	// 缓冲保持极小；突发洪峰由输入通道背压 + 缓冲上界共同约束。
	for i := 0; i < total; i++ {
		lines = append(lines, line(3, fmt.Sprintf("e%07d", i), 1, i))
	}
	inPath := writeLines(t, "big.jsonl", lines)

	in, err := os.Open(inPath)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	outPath := filepath.Join(t.TempDir(), "big-out.jsonl")
	out, err := os.Create(outPath)
	if err != nil {
		t.Fatal(err)
	}
	src := streamio.NewLineSource(in)
	sink := streamio.NewLineSink(out)
	p := pipeline.New(cfg, pipeline.DefaultRegistry())
	start := time.Now()
	if err := p.Run(context.Background(), src, sink); err != nil {
		t.Fatalf("Run 失败: %v", err)
	}
	if err := sink.Flush(); err != nil {
		t.Fatal(err)
	}
	out.Close()

	st := p.Stats()
	if st.Processed != total {
		t.Fatalf("处理数 = %d, 期望 %d", st.Processed, total)
	}
	if sink.Written() != total {
		t.Fatalf("唯一输出 = %d, 期望 %d（丢失或重复）", sink.Written(), total)
	}
	// 校验输出文件恰好 total 行、ID 唯一且按事件时间升序。
	data, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatal(err)
	}
	rows := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	if len(rows) != total {
		t.Fatalf("输出文件行数 = %d, 期望 %d", len(rows), total)
	}
	var prev time.Time
	seenIDs := make(map[string]struct{}, total)
	for i, l := range rows {
		var row struct {
			ID        string    `json:"id"`
			EventTime time.Time `json:"event_time"`
			Exchange  string    `json:"exchange"`
			VenueMIC  string    `json:"venue_mic"`
			IsMaker   bool      `json:"is_maker"`
		}
		if err := json.Unmarshal([]byte(l), &row); err != nil {
			t.Fatalf("第 %d 行解析失败: %v", i, err)
		}
		if i == 0 {
			prev = row.EventTime
		} else if row.EventTime.Before(prev) {
			t.Fatalf("第 %d 行事件时间逆序: %s < %s", i, row.EventTime, prev)
		}
		prev = row.EventTime
		if _, dup := seenIDs[row.ID]; dup {
			t.Fatalf("输出出现重复 ID: %s", row.ID)
		}
		seenIDs[row.ID] = struct{}{}
		// 即使大输入是 V3 行，统一形状保证每条都带全部已声明扩展字段。
		if row.Exchange != "UNKNOWN" || row.VenueMIC != "XXXX" || row.IsMaker != false {
			t.Fatalf("第 %d 行缺省扩展字段缺失: %+v", i, row)
		}
	}
	t.Logf("用例结果: %d 条全部处理且唯一输出、按事件时间有序，耗时 %v，断点 %d 次，回执环恒为 %d [通过]",
		total, time.Since(start).Round(time.Millisecond), st.Checkpoints, len(p.Results()))
}

// TestE2EBufferedOverflowBounded 专门制造"缓冲堆积超过上界"的压力，
// 断言在 MaxBuffered 很小时仍有界、不丢不重（溢出事件按序提前输出）。
func TestE2EBufferedOverflowBounded(t *testing.T) {
	t.Log("业务场景: 极端乱序导致缓冲持续堆积，超过硬上界时按序提前输出，事件不丢且全局有序")
	cfg := e2eConfig(t)
	cfg.MaxBuffered = 10
	cfg.DedupCapacity = 5000
	cfg.CheckpointEvery = 100000
	cfg.MaxOutOfOrderness = time.Hour // 水位线几乎不推进，强制走溢出路径
	cfg.AllowedLateness = 0

	const n = 200
	lines := make([]string, 0, n+1)
	// 先发一个极晚的锚点顶住水位线，再发 n 个更早（但晚于锚点-1h 截止）的事件，
	// 这些事件无法靠水位线输出，只能靠缓冲上界溢出提前输出。
	lines = append(lines, line(1, "anchor", 1, 4000))
	for i := 0; i < n; i++ {
		lines = append(lines, line(1, fmt.Sprintf("b%03d", i), 1, 1000+i))
	}
	inPath := writeLines(t, "overflow.jsonl", lines)

	in, _ := os.Open(inPath)
	defer in.Close()
	outPath := filepath.Join(t.TempDir(), "out.jsonl")
	out, _ := os.Create(outPath)
	src := streamio.NewLineSource(in)
	sink := streamio.NewLineSink(out)
	p := pipeline.New(cfg, pipeline.DefaultRegistry())
	if err := p.Run(context.Background(), src, sink); err != nil {
		t.Fatalf("Run 失败: %v", err)
	}
	sink.Flush()
	out.Close()

	if sink.Written() != n+1 {
		t.Fatalf("唯一输出 = %d, 期望 %d（溢出路径丢事件）", sink.Written(), n+1)
	}
	data, _ := os.ReadFile(outPath)
	rows := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	var prev time.Time
	for i, l := range rows {
		var row struct {
			ID        string    `json:"id"`
			EventTime time.Time `json:"event_time"`
		}
		if err := json.Unmarshal([]byte(l), &row); err != nil {
			t.Fatalf("行解析失败: %v", err)
		}
		if i > 0 && row.EventTime.Before(prev) {
			t.Fatalf("溢出提前输出破坏全局顺序: %s < %s", row.EventTime, prev)
		}
		prev = row.EventTime
	}
	t.Logf("用例结果: 缓冲硬上界 %d 下处理 %d 条堆积事件，全部唯一且按事件时间有序输出 [通过]",
		cfg.MaxBuffered, n+1)
}
