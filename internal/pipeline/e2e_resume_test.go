package pipeline_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/feng111627/market-event-pipeline-go/internal/event"
	"github.com/feng111627/market-event-pipeline-go/internal/pipeline"
	"github.com/feng111627/market-event-pipeline-go/internal/streamio"
)

// crashSink 包装 LineSink，在第 failAfter 次 Emit 时取消上下文模拟崩溃。
type crashSink struct {
	inner     *streamio.LineSink
	emits     int64
	failAfter int64
	cancel    func()
}

func (s *crashSink) Emit(ev event.MarketEvent) error {
	if atomic.AddInt64(&s.emits, 1) == s.failAfter {
		s.cancel()
	}
	return s.inner.Emit(ev)
}

// readOutput 读取 JSONL 输出文件为 map 切片。
func readOutput(t *testing.T, path string) []map[string]interface{} {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var rows []map[string]interface{}
	for _, l := range strings.Split(strings.TrimRight(string(data), "\n"), "\n") {
		if strings.TrimSpace(l) == "" {
			continue
		}
		var row map[string]interface{}
		if err := json.Unmarshal([]byte(l), &row); err != nil {
			t.Fatalf("输出行非 JSON: %v (%s)", err, l)
		}
		rows = append(rows, row)
	}
	return rows
}

// TestE2EFileCrashResume 用真实文件源/文件 Sink 验证：处理中途崩溃后带断点重跑，
// 已提交位点之前的行不重读，未提交的重放由接收端幂等吸收，最终恰好一次；
// 滞留在缓冲中的事件及其扩展字段随断点快照完整往返。
func TestE2EFileCrashResume(t *testing.T) {
	t.Log("业务场景: 真实文件跑到一半崩溃，重启按字节位点从断点续跑，最终结果不多不少且扩展字段完整")
	cfg := e2eConfig(t)
	cfg.CheckpointEvery = 7
	cfg.MaxOutOfOrderness = 3 * time.Second
	cfg.AllowedLateness = 1

	const total = 40
	var lines []string
	for i := 0; i < total; i++ {
		// 轻微乱序，使部分事件在崩溃时仍滞留缓冲（验证缓冲快照含扩展字段）。
		sec := i
		if i%5 == 1 && i > 1 {
			sec = i - 1
		}
		lines = append(lines, line(3, fmt.Sprintf("r%02d", i), 1, sec,
			"exchange", `"CB"`, "venue_mic", `"XNMS"`, "is_maker", `true`))
	}
	inPath := writeLines(t, "resume-in.jsonl", lines)
	out1Path := filepath.Join(t.TempDir(), "run1.jsonl")
	out2Path := filepath.Join(t.TempDir(), "run2.jsonl")

	openIn := func() *os.File {
		f, err := os.Open(inPath)
		if err != nil {
			t.Fatal(err)
		}
		return f
	}

	// 第一次运行：在第 15 次 Emit 时取消（崩溃点落在两次断点之间）。
	ctx, cancel := context.WithCancel(context.Background())
	in1 := openIn()
	out1, err := os.Create(out1Path)
	if err != nil {
		t.Fatal(err)
	}
	sink1 := &crashSink{inner: streamio.NewLineSink(out1), failAfter: 15, cancel: cancel}
	p1 := pipeline.New(cfg, pipeline.DefaultRegistry())
	runErr := p1.Run(ctx, streamio.NewLineSource(in1), sink1)
	if runErr == nil {
		t.Fatal("第一次运行应被取消中断")
	}
	if err := sink1.inner.Flush(); err != nil {
		t.Fatal(err)
	}
	out1.Close()
	in1.Close()
	t.Logf("第一次运行中断: Emit 调用 %d 次（含未入断点的少量输出）", atomic.LoadInt64(&sink1.emits))

	// 第二次运行：新进程（新 Pipeline/Source/Sink），从断点按字节位点续跑。
	in2 := openIn()
	out2, err := os.Create(out2Path)
	if err != nil {
		t.Fatal(err)
	}
	sink2 := streamio.NewLineSink(out2)
	p2 := pipeline.New(cfg, pipeline.DefaultRegistry())
	if err := p2.Resume(context.Background(), streamio.NewLineSource(in2), sink2, -1); err != nil {
		t.Fatalf("Resume 失败: %v", err)
	}
	if err := sink2.Flush(); err != nil {
		t.Fatal(err)
	}
	st2 := p2.Stats()
	out2.Close()
	in2.Close()

	// 合并两段输出：第一次运行的输出 + 续跑输出。已提交位点之前的行不重读，
	// 因此并集中每个 ID 恰好一次（重放只可能出现在两段交界处，由接收端幂等去重）。
	rows1 := readOutput(t, out1Path)
	rows2 := readOutput(t, out2Path)
	union := map[string]map[string]interface{}{}
	for _, rows := range [][]map[string]interface{}{rows1, rows2} {
		for _, row := range rows {
			id := row["id"].(string)
			if _, dup := union[id]; dup {
				// 交界处重放：同一 ID 内容必须一致（幂等可安全吸收）。
				if fmt.Sprint(union[id]) != fmt.Sprint(row) {
					t.Fatalf("重放事件 %s 内容不一致", id)
				}
				continue
			}
			union[id] = row
		}
	}
	if len(union) != total {
		t.Fatalf("两段输出并集唯一 ID = %d, 期望 %d（不多不少失败）", len(union), total)
	}
	// 续跑段自身应按事件时间有序，且扩展字段完整。
	var prev time.Time
	for i, row := range rows2 {
		if row["exchange"] != "CB" || row["venue_mic"] != "XNMS" || row["is_maker"] != true {
			t.Fatalf("恢复后扩展字段丢失: %+v", row)
		}
		et, _ := time.Parse(time.RFC3339Nano, row["event_time"].(string))
		if i > 0 && et.Before(prev) {
			t.Fatalf("续跑段事件时间逆序: %s < %s", et, prev)
		}
		prev = et
	}
	t.Logf("用例结果: 崩溃续跑并集唯一 %d 条（第一段 %d + 续跑 %d，续跑处理 %d 条），扩展字段完整、有序 [通过]",
		len(union), len(rows1), len(rows2), st2.Processed)
}
