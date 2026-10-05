// Command demo 在本地运行行情事件流：读取 JSONL，按事件时间输出结果，并维护可恢复断点。
//
// 用法示例：
//
//	go run ./cmd/demo -input testdata/sample.jsonl -out /tmp/out.jsonl \
//	    -checkpoint /tmp/cp.json -epoch 1
//
// 中断后用相同 -epoch 重跑即可从断点恢复；更换 -epoch 或断点超过 -ttl 会被明确拒绝。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/feng111627/market-event-pipeline-go/internal/checkpoint"
	"github.com/feng111627/market-event-pipeline-go/internal/event"
	"github.com/feng111627/market-event-pipeline-go/internal/ingress"
	"github.com/feng111627/market-event-pipeline-go/internal/pipeline"
	"github.com/feng111627/market-event-pipeline-go/internal/schema"
	"github.com/feng111627/market-event-pipeline-go/internal/sink"
)

func main() {
	var (
		input       = flag.String("input", "testdata/sample.jsonl", "输入 JSONL 文件（行情记录或 watermark 控制记录）")
		out         = flag.String("out", "out.jsonl", "结果输出 JSONL 文件")
		cpPath      = flag.String("checkpoint", "checkpoint.json", "断点文件路径")
		epoch       = flag.Int64("epoch", 1, "启动纪元；与断点不一致则拒绝恢复（用于防止跨代/陈旧断点）")
		ttl         = flag.Duration("ttl", time.Hour, "断点有效期；超过则拒绝恢复（0 表示不检查时间）")
		lateness    = flag.Duration("lateness", 2*time.Second, "乱序容忍窗口")
		bufferLimit = flag.Int("buffer-limit", 10000, "排序缓冲条数上限（背压阈值）")
		cpEvery     = flag.Duration("checkpoint-every", 0, "周期断点间隔（0=每条记录处理后提交）")
	)
	flag.Parse()
	logger := log.New(os.Stdout, "[pipeline] ", log.LstdFlags|log.Lmicroseconds)

	reg := defaultRegistry()

	sk, err := sink.OpenFileSink(*out)
	if err != nil {
		logger.Fatalf("open sink: %v", err)
	}
	defer sk.Close()

	cp := checkpoint.NewFileStore(*cpPath)
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	startOffset := int64(0)
	state, err := cp.Load(ctx)
	switch {
	case errors.Is(err, nil):
		val := checkpoint.NewValidator(*ttl)
		if verr := val.Validate(state, *epoch, sk.AcknowledgedSeq(), time.Now()); verr != nil {
			logger.Fatalf("resume rejected: %v", verr)
		}
		startOffset = state.InputOffset
		logger.Printf("resume from checkpoint offset=%d watermark=%s sink_seq=%d",
			state.InputOffset, time.Unix(0, state.WatermarkUnix).Format(time.RFC3339Nano), state.SinkSeq)
	case errors.Is(err, checkpoint.ErrNoCheckpoint):
		logger.Printf("no checkpoint found, start from beginning")
	default:
		logger.Fatalf("load checkpoint: %v", err)
	}

	in, err := os.Open(*input)
	if err != nil {
		logger.Fatalf("open input: %v", err)
	}
	defer in.Close()

	var reader *ingress.Reader
	if startOffset > 0 {
		reader, err = ingress.NewReaderAt(in, startOffset)
		if err != nil {
			logger.Fatalf("seek to %d: %v", startOffset, err)
		}
	} else {
		reader = ingress.NewReader(in)
	}

	p := pipeline.New(pipeline.Config{
		AllowedLateness:   *lateness,
		BufferLimit:       *bufferLimit,
		InputQueue:        256,
		CheckpointEvery:   *cpEvery,
		CheckpointTTL:     *ttl,
		Epoch:             *epoch,
		WatermarkInterval: 50 * time.Millisecond,
	}, reg, sk, cp, event.SystemClock{}, logger)
	if state != nil {
		if err := p.Restore(state); err != nil {
			logger.Fatalf("restore: %v", err)
		}
	}

	stats, err := p.Run(ctx, reader)
	fmt.Printf("\n==== stats ====\n")
	fmt.Printf("accepted=%d merged=%d buffered=%d late_dropped=%d duplicate=%d conflict_rejected=%d schema_rejected=%d backpressured=%d watermark_ctrl=%d control_rejected=%d\n",
		stats.Accepted, stats.Merged, stats.Buffered, stats.DroppedLate, stats.Duplicate,
		stats.ConflictRejected, stats.SchemaRejected, stats.Backpressured, stats.WatermarkCtrl, stats.ControlRejected)
	if err != nil && !errors.Is(err, context.Canceled) {
		logger.Fatalf("run: %v", err)
	}
	logger.Printf("done, output=%s checkpoint=%s", *out, *cpPath)
}

// defaultRegistry 注册示例结构：
//
//	v1: 核心字段 + 无 extra
//	v2: 增加可选字段 bid_size（缺省 0）——加法兼容演进
func defaultRegistry() *schema.Registry {
	reg := schema.NewRegistry()
	must(reg.Register(1, nil))
	must(reg.Register(2, []schema.FieldDef{
		{Name: "bid_size", Kind: schema.KindInt64, Required: false, Default: 0, HasDefault: true},
	}))
	return reg
}

func must(err error) {
	if err != nil {
		log.Fatalf("register schema: %v", err)
	}
}
