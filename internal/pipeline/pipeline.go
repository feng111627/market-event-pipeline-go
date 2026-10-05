// Package pipeline 编排行情事件的接收、校验、去重、重排与断点提交。
package pipeline

import (
	"context"
	"time"

	"github.com/feng111627/market-event-pipeline-go/internal/checkpoint"
	"github.com/feng111627/market-event-pipeline-go/internal/event"
)

// Config 是流水线配置。
type Config struct {
	MaxOutOfOrderness time.Duration // 允许的最大乱序时长
	AllowedLateness   time.Duration // 水位线后仍允许的迟到宽限
	DedupCapacity     int           // 去重表容量
	InputBufferSize   int           // 输入通道容量（背压边界）
	CheckpointEvery   int           // 每处理多少条提交一次断点
	CheckpointPath    string        // 断点文件路径
	CheckpointTTL     time.Duration // 断点有效期
}

// SourceRecord 是上游投递的一条原始记录。
type SourceRecord struct {
	Offset        int64
	SchemaVersion int
	Payload       map[string]interface{}
}

// Source 抽象上游数据源。
type Source interface {
	// Next 返回下一条记录；ok=false 表示源已耗尽。
	Next() (SourceRecord, bool)
	// SeekTo 将源定位到指定位点（用于断点恢复）。
	SeekTo(offset int64) error
}

// Sink 抽象下游输出。
type Sink interface {
	// Emit 输出一条按事件时间排序后的事件。
	Emit(ev event.MarketEvent) error
}

// Stats 汇总运行期统计，供测试与观测断言。
type Stats struct {
	Accepted    int
	Duplicates  int
	Stales      int
	Merged      int
	LateDropped int
	Rejected    int
	Emitted     int
	Checkpoints int
}

// Pipeline 是行情事件处理流水线。
type Pipeline struct {
	cfg   Config
	stats Stats
}

// New 创建流水线。
func New(cfg Config) *Pipeline {
	return &Pipeline{cfg: cfg}
}

// Stats 返回当前统计快照。
func (p *Pipeline) Stats() Stats {
	return p.stats
}

// Run 从源消费事件直到源耗尽或 ctx 取消，按事件时间向 sink 输出。
// 背压：输入通道容量固定，突发流量时接收端阻塞而不是无限堆积。
func (p *Pipeline) Run(ctx context.Context, src Source, sink Sink) error {
	return nil
}

// Resume 从断点恢复后继续运行；断点过期或位点回退时返回明确错误。
func (p *Pipeline) Resume(ctx context.Context, src Source, sink Sink) error {
	return nil
}

// checkpoint 返回当前断点状态（供测试与内部提交使用）。
func (p *Pipeline) checkpointState() checkpoint.State {
	return checkpoint.State{}
}
