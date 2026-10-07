// market-pipeline 是行情事件流处理组件的命令行入口。
//
// 从文件或标准输入按行读取带结构版本的 JSON 行情记录，按事件时间
// 清洗、去重、重排后有序写入文件或标准输出，并打印本次处理汇总。
//
// 用法示例：
//
//	# 文件 -> 文件
//	market-pipeline -input ticks.jsonl -output out.jsonl
//	# 标准输入 -> 标准输出（一次性，不做断点恢复）
//	cat ticks.jsonl | market-pipeline
//	# 内置演示
//	market-pipeline -demo
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/feng111627/market-event-pipeline-go/internal/jsonio"
	"github.com/feng111627/market-event-pipeline-go/internal/pipeline"
	"github.com/feng111627/market-event-pipeline-go/internal/schema"
)

type options struct {
	demo         bool
	input        string
	output       string
	checkpoint   string
	noCheckpoint bool
	oooo         time.Duration
	lateness     time.Duration
	maxBuffer    int
	dedupCap     int
	bufSize      int
	cpEvery      int
	ttl          time.Duration
}

func parseFlags() options {
	var o options
	flag.BoolVar(&o.demo, "demo", false, "运行内置演示样例")
	flag.StringVar(&o.input, "input", jsonio.StdinPath, "输入文件路径（- 表示标准输入）")
	flag.StringVar(&o.output, "output", jsonio.StdoutPath, "输出文件路径（- 表示标准输出）")
	flag.StringVar(&o.checkpoint, "checkpoint", "data/checkpoint.json", "断点文件路径")
	flag.BoolVar(&o.noCheckpoint, "no-checkpoint", false, "禁用断点持久化（一次性运行）")
	flag.DurationVar(&o.oooo, "max-out-of-order", 5*time.Second, "允许的最大乱序时长")
	flag.DurationVar(&o.lateness, "allowed-lateness", 1*time.Second, "水位线后允许的迟到宽限")
	flag.IntVar(&o.maxBuffer, "max-buffer", 100000, "乱序重排缓冲容量硬上界（内存上界，<=0 不限）")
	flag.IntVar(&o.dedupCap, "dedup-capacity", 100000, "去重表容量上界")
	flag.IntVar(&o.bufSize, "input-buffer", 1024, "输入通道容量（背压边界）")
	flag.IntVar(&o.cpEvery, "checkpoint-every", 500, "每处理多少行提交一次断点")
	flag.DurationVar(&o.ttl, "checkpoint-ttl", 24*time.Hour, "断点有效期（0 表示不过期）")
	flag.Parse()
	return o
}

// registry 注册当前支持的全部兼容结构版本。
func registry() *schema.Registry {
	reg := schema.NewRegistry()
	for _, s := range []schema.Schema{
		pipeline.MarketEventSchemaV1(),
		pipeline.MarketEventSchemaV2(),
		pipeline.MarketEventSchemaV3(),
	} {
		if err := reg.Register(s); err != nil {
			log.Fatalf("注册结构失败: %v", err)
		}
	}
	return reg
}

func main() {
	o := parseFlags()
	if o.demo {
		if err := runDemo(o.checkpoint); err != nil {
			log.Fatal(err)
		}
		return
	}
	if err := runBatch(o); err != nil {
		log.Fatal(err)
	}
}

func runBatch(o options) error {
	cfg := pipeline.DefaultConfig()
	cfg.MaxOutOfOrderness = o.oooo
	cfg.AllowedLateness = o.lateness
	cfg.MaxBufferSize = o.maxBuffer
	cfg.DedupCapacity = o.dedupCap
	cfg.InputBufferSize = o.bufSize
	cfg.CheckpointEvery = o.cpEvery
	cfg.CheckpointTTL = o.ttl

	stdin := o.input == jsonio.StdinPath
	stdout := o.output == jsonio.StdoutPath
	// 标准输入不可重读、标准输出不可截断，二者参与的运行都是一次性的，
	// 不做断点，也就不会出现"重跑得到半份结果"的歧义。
	ephemeral := o.noCheckpoint || stdin || stdout
	if ephemeral {
		cfg.CheckpointPath = ""
	} else {
		cfg.CheckpointPath = o.checkpoint
	}

	src, err := jsonio.OpenFileSource(o.input)
	if err != nil {
		return err
	}
	defer src.Close()

	sink, err := jsonio.OpenFileSink(o.output)
	if err != nil {
		return err
	}
	defer sink.Close()

	p := pipeline.New(cfg, registry())
	if err := p.Run(context.Background(), src, sink); err != nil {
		return fmt.Errorf("流水线运行失败: %w", err)
	}

	// 汇总打到标准错误：输出到 stdout 时数据与报告分流，便于下游只取数据。
	printSummaryTo(os.Stderr, o, p.Stats())
	return nil
}

// summaryLine 用中文标签给出每种处置结论的计数。
func summaryLines(st pipeline.Stats) []string {
	total := st.Accepted + st.Duplicates + st.Stales + st.Merged + st.LateDropped + st.Rejected
	return []string{
		fmt.Sprintf("输入总行数(processed)   : %d", st.Processed),
		fmt.Sprintf("处置合计                : %d", total),
		fmt.Sprintf("正常接收 ACCEPTED       : %d", st.Accepted),
		fmt.Sprintf("重复丢弃 DUPLICATE      : %d", st.Duplicates),
		fmt.Sprintf("过期丢弃 STALE          : %d", st.Stales),
		fmt.Sprintf("合并更正 MERGED         : %d", st.Merged),
		fmt.Sprintf("迟到丢弃 LATE_DROPPED   : %d", st.LateDropped),
		fmt.Sprintf("明确拒绝 REJECTED       : %d", st.Rejected),
		fmt.Sprintf("最终输出行数(emitted)   : %d", st.Emitted),
		fmt.Sprintf("缓冲超限强制输出        : %d", st.ForcedEmitted),
		fmt.Sprintf("乱序缓冲峰值            : %d", st.BufferedMax),
		fmt.Sprintf("断点提交次数            : %d", st.Checkpoints),
	}
}

func printSummary(st pipeline.Stats) { printSummaryTo(os.Stdout, options{demo: true}, st) }

func printSummaryTo(w io.Writer, o options, st pipeline.Stats) {
	fmt.Fprintln(w, "--- 运行汇总 ---")
	for _, l := range summaryLines(st) {
		fmt.Fprintln(w, l)
	}
	if !o.demo {
		mode := "持久化(断点恢复)"
		if ephemeral := o.noCheckpoint || o.input == jsonio.StdinPath || o.output == jsonio.StdoutPath; ephemeral {
			mode = "一次性(无断点：stdin/stdout 或显式禁用)"
		}
		fmt.Fprintf(w, "运行模式                : %s\n", mode)
	}
}
