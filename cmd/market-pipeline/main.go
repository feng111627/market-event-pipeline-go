// market-pipeline 是行情事件流处理组件的可运行入口。
//
// 两种用法：
//
//	# 1) 处理你自己的数据（JSONL，每行 {"version":N,"payload":{...}} 或平铺）
//	market-pipeline -in data.jsonl -out out.jsonl
//	cat data.jsonl | market-pipeline            # 标准输入 -> 标准输出
//
//	# 2) 内置演示（含乱序/重复/更正/迟到样例）
//	market-pipeline -demo
//
// 整批跑完后向标准错误打印一份汇总：总处理数与每种处置结论各多少。
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/feng111627/market-event-pipeline-go/internal/event"
	"github.com/feng111627/market-event-pipeline-go/internal/pipeline"
	"github.com/feng111627/market-event-pipeline-go/internal/schema"
	"github.com/feng111627/market-event-pipeline-go/internal/streamio"
)

var base = time.Date(2026, 10, 6, 10, 0, 0, 0, time.UTC)

func main() {
	var (
		inPath     = flag.String("in", "", "输入 JSONL 文件；缺省或 \"-\" 表示标准输入")
		outPath    = flag.String("out", "", "输出 JSONL 文件；缺省或 \"-\" 表示标准输出")
		cpPath     = flag.String("checkpoint", "data/checkpoint.json", "断点文件路径")
		resume     = flag.Bool("resume", false, "从已有断点续跑（默认忽略旧断点从头开始并覆盖它）")
		demo       = flag.Bool("demo", false, "运行内置演示数据")
		maxBuf     = flag.Int("max-buffered", 0, "乱序缓冲硬上界（<=0 用默认值；负数不限制）")
		dedupCap   = flag.Int("dedup-capacity", 0, "去重表容量（<=0 用默认值）")
		inputBuf   = flag.Int("input-buffer", 0, "输入通道容量（背压边界，<=0 用默认值）")
		cpEvery    = flag.Int("checkpoint-every", 0, "每处理多少条提交一次断点（<=0 用默认值）")
		cpTTLHours = flag.Float64("checkpoint-ttl-hours", 0, "断点有效期小时数（<=0 用默认值）")
	)
	flag.Parse()

	reg := pipeline.DefaultRegistry()
	cfg := pipeline.DefaultConfig()
	cfg.CheckpointPath = *cpPath
	if *maxBuf != 0 {
		cfg.MaxBuffered = *maxBuf
	}
	if *dedupCap != 0 {
		cfg.DedupCapacity = *dedupCap
	}
	if *inputBuf != 0 {
		cfg.InputBufferSize = *inputBuf
	}
	if *cpEvery != 0 {
		cfg.CheckpointEvery = *cpEvery
	}
	if *cpTTLHours > 0 {
		cfg.CheckpointTTL = time.Duration(*cpTTLHours * float64(time.Hour))
	} else if *cpTTLHours < 0 {
		cfg.CheckpointTTL = 0
	}

	if *demo {
		runDemo(reg, cfg)
		return
	}
	if err := runStream(*inPath, *outPath, *resume, reg, cfg); err != nil {
		log.Fatalf("运行失败: %v", err)
	}
}

// openInput 打开输入：缺省/"-" 用标准输入；否则打开文件（支持按字节断点恢复）。
func openInput(path string) (io.ReadCloser, func(), error) {
	if path == "" || path == "-" {
		return io.NopCloser(os.Stdin), func() {}, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	return f, func() { f.Close() }, nil
}

// openOutput 打开输出：缺省/"-" 用标准输出；否则创建/截断文件。
func openOutput(path string) (io.WriteCloser, func(), error) {
	if path == "" || path == "-" {
		return nopWriteCloser{os.Stdout}, func() {}, nil
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, nil, err
	}
	return f, func() { f.Close() }, nil
}

type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }

// runStream 从外部文件/标准输入跑到文件/标准输出，并打印汇总。
func runStream(inPath, outPath string, resume bool, reg *schema.Registry, cfg pipeline.Config) error {
	in, closeIn, err := openInput(inPath)
	if err != nil {
		return fmt.Errorf("打开输入: %w", err)
	}
	defer closeIn()
	out, closeOut, err := openOutput(outPath)
	if err != nil {
		return fmt.Errorf("打开输出: %w", err)
	}
	defer closeOut()

	src := streamio.NewLineSource(in)
	sink := streamio.NewLineSink(out)

	p := pipeline.New(cfg, reg)
	ctx := context.Background()
	if resume {
		if err := p.Resume(ctx, src, sink, -1); err != nil {
			return err
		}
	} else {
		// 从头开始：忽略并覆盖可能存在的旧断点，避免被自动恢复带到文件尾。
		if err := os.Remove(cfg.CheckpointPath); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("清理旧断点: %w", err)
		}
		if err := p.Run(ctx, src, sink); err != nil {
			return err
		}
	}
	if err := sink.Flush(); err != nil {
		return fmt.Errorf("刷新输出: %w", err)
	}

	sum := p.Summary()
	bad := src.BadLines()
	printSummary(os.Stderr, inPath, outPath, sum, bad)
	return nil
}

// printSummary 向 w 打印整批汇总。
func printSummary(w io.Writer, inPath, outPath string, s pipeline.Summary, bad []streamio.BadLine) {
	in := inPath
	if in == "" || in == "-" {
		in = "<标准输入>"
	}
	out := outPath
	if out == "" || out == "-" {
		out = "<标准输出>"
	}
	fmt.Fprintf(w, "\n===== 运行汇总 =====\n")
	fmt.Fprintf(w, "输入: %s  输出: %s\n", in, out)
	fmt.Fprintf(w, "总处理记录: %d\n", s.Processed)
	fmt.Fprintf(w, "  正常接收 ACCEPTED          : %d\n", s.Accepted)
	fmt.Fprintf(w, "  重复丢弃 DUPLICATE_DROPPED : %d\n", s.DuplicateDropped)
	fmt.Fprintf(w, "  过期丢弃 STALE_DROPPED     : %d\n", s.StaleDropped)
	fmt.Fprintf(w, "  合并更正 MERGED            : %d\n", s.Merged)
	fmt.Fprintf(w, "  过期/迟到 LATE_DROPPED     : %d\n", s.LateDropped)
	fmt.Fprintf(w, "  明确拒绝 REJECTED          : %d\n", s.Rejected)
	fmt.Fprintf(w, "实际输出(唯一) EMITTED      : %d\n", s.Emitted)
	fmt.Fprintf(w, "断点提交次数                 : %d\n", s.Checkpoints)
	if len(bad) > 0 {
		fmt.Fprintf(w, "无法解析的输入行             : %d（不进入流水线）\n", len(bad))
		for _, b := range bad {
			preview := b.Raw
			if len(preview) > 120 {
				preview = preview[:120] + "..."
			}
			fmt.Fprintf(w, "  行 %d: %s (%s)\n", b.Line, b.Reason, preview)
		}
	}
}

// ---------------- 内置演示（保持原有能力可直接运行） ----------------

type demoSource struct {
	recs []pipeline.SourceRecord
	pos  int64
}

func (s *demoSource) Next() (pipeline.SourceRecord, bool) {
	if int(s.pos) >= len(s.recs) {
		return pipeline.SourceRecord{}, false
	}
	r := s.recs[s.pos]
	s.pos++
	return r, true
}

func (s *demoSource) SeekTo(offset int64) error { s.pos = offset; return nil }

type printSink struct{ n int }

func (p *printSink) Emit(ev event.MarketEvent) error {
	p.n++
	fmt.Printf("[输出 #%02d] v%d id=%s symbol=%s price=%.2f vol=%.2f event_time=%s attrs=%v\n",
		p.n, ev.SchemaVer, ev.ID, ev.Symbol, ev.Price, ev.Volume,
		ev.EventTime.Format("15:04:05"), ev.Attrs)
	return nil
}

func mkRec(offset int64, version int, id string, seq int64, sec int, price float64) pipeline.SourceRecord {
	return pipeline.SourceRecord{
		Offset:        offset,
		SchemaVersion: version,
		Payload: map[string]interface{}{
			"id": id, "symbol": "BTCUSDT", "price": price, "volume": 1.5,
			"seq": seq, "event_time": base.Add(time.Duration(sec) * time.Second),
		},
	}
}

func runDemo(reg *schema.Registry, cfg pipeline.Config) {
	src := &demoSource{recs: []pipeline.SourceRecord{
		mkRec(0, 1, "tick-03", 1, 13, 30100),
		mkRec(1, 1, "tick-01", 1, 11, 30000), // 乱序：窗口内，等待
		mkRec(2, 1, "tick-01", 1, 11, 30000), // 重复投递：丢弃
		mkRec(3, 1, "tick-02", 1, 12, 30050),
		mkRec(4, 1, "tick-02", 2, 12, 30055), // 更正：合并替换
		mkRec(5, 2, "tick-04", 1, 14, 30120), // V2，exchange 取缺省值
		mkRec(6, 3, "tick-06", 1, 15, 30130), // V3，三个新增字段取缺省值
		mkRec(7, 1, "tick-05", 1, 25, 30200), // 推进水位线，弹出前面事件
		mkRec(8, 1, "tick-00", 1, 10, 29900), // 迟到：丢弃
	}}
	p := pipeline.New(cfg, reg)
	sink := &printSink{}
	if err := p.Run(context.Background(), src, sink); err != nil {
		log.Fatalf("流水线运行失败: %v", err)
	}
	fmt.Println("\n--- 单条处置回执 ---")
	for _, r := range p.Results() {
		fmt.Printf("id=%-8s 结论=%-18s 原因=%s\n", r.EventID, r.Disposition, r.Reason)
	}
	fmt.Printf("\n--- 运行统计 ---\n%+v\n", p.Stats())
	fmt.Printf("断点已写入: %s\n", cfg.CheckpointPath)
}
