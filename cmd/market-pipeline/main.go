// market-pipeline 是行情事件流处理组件的可运行演示入口。
// 它构造一批含乱序、重复、更正与迟到的模拟行情事件，
// 运行流水线并打印每条事件的处置结论与最终按事件时间排序的输出。
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/feng111627/market-event-pipeline-go/internal/event"
	"github.com/feng111627/market-event-pipeline-go/internal/pipeline"
	"github.com/feng111627/market-event-pipeline-go/internal/schema"
)

var base = time.Date(2026, 10, 6, 10, 0, 0, 0, time.UTC)

// demoSource 依次吐出预置的模拟记录。
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

func (s *demoSource) SeekTo(offset int64) error {
	s.pos = offset
	return nil
}

// printSink 打印每条按事件时间输出的行情事件。
type printSink struct{ n int }

func (p *printSink) Emit(ev event.MarketEvent) error {
	p.n++
	fmt.Printf("[输出 #%02d] id=%s symbol=%s price=%.2f vol=%.2f event_time=%s\n",
		p.n, ev.ID, ev.Symbol, ev.Price, ev.Volume, ev.EventTime.Format("15:04:05"))
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

func main() {
	cpPath := flag.String("checkpoint", "data/checkpoint.json", "断点文件路径")
	flag.Parse()

	reg := schema.NewRegistry()
	for _, s := range []schema.Schema{pipeline.MarketEventSchemaV1(), pipeline.MarketEventSchemaV2()} {
		if err := reg.Register(s); err != nil {
			log.Fatalf("注册结构失败: %v", err)
		}
	}

	cfg := pipeline.DefaultConfig()
	cfg.CheckpointPath = *cpPath

	// 模拟上游：乱序、重复投递、版本更正、迟到、V2 结构（缺新字段）。
	src := &demoSource{recs: []pipeline.SourceRecord{
		mkRec(0, 1, "tick-03", 1, 13, 30100),
		mkRec(1, 1, "tick-01", 1, 11, 30000), // 乱序：窗口内，等待
		mkRec(2, 1, "tick-01", 1, 11, 30000), // 重复投递：丢弃
		mkRec(3, 1, "tick-02", 1, 12, 30050),
		mkRec(4, 1, "tick-02", 2, 12, 30055), // 更正：合并替换
		mkRec(5, 2, "tick-04", 1, 14, 30120), // V2 结构，exchange 取缺省值
		mkRec(6, 1, "tick-05", 1, 25, 30200), // 推进水位线，弹出前面事件
		mkRec(7, 1, "tick-00", 1, 10, 29900), // 迟到：丢弃
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
	fmt.Printf("断点已写入: %s\n", *cpPath)

	var _ = event.Accepted // 保持 event 包引用（处置枚举见回执输出）
}
