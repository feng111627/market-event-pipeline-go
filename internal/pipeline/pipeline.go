// Package pipeline 编排行情事件的接收、校验、去重、重排与断点提交。
//
// 处理顺序：结构校验(Normalize) -> 去重判定(dedup) -> 迟到判定(watermark)
// -> 乱序缓冲 -> 水位线推进后按事件时间输出。
//
// 背压：输入通道容量固定（InputBufferSize），突发流量时读取端阻塞，
// 不会在内存中无限堆积。
//
// 断点：每处理 CheckpointEvery 条提交一次（位点 + 水位线基准 + 去重状态）。
// 崩溃恢复后从已提交位点重放，Sink 必须按事件 ID 幂等（见 Sink 注释），
// 从而保证最终结果不多也不少。
package pipeline

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/feng111627/market-event-pipeline-go/internal/checkpoint"
	"github.com/feng111627/market-event-pipeline-go/internal/dedup"
	"github.com/feng111627/market-event-pipeline-go/internal/event"
	"github.com/feng111627/market-event-pipeline-go/internal/schema"
	"github.com/feng111627/market-event-pipeline-go/internal/watermark"
)

// 行情事件的标准字段名。
const (
	FieldID        = "id"
	FieldSymbol    = "symbol"
	FieldPrice     = "price"
	FieldVolume    = "volume"
	FieldSeq       = "seq"
	FieldEventTime = "event_time"
)

// MarketEventSchemaV1 返回行情事件的基础结构。
func MarketEventSchemaV1() schema.Schema {
	return schema.Schema{
		Version: 1,
		Fields: []schema.Field{
			{Name: FieldID, Type: schema.TypeString, Required: true},
			{Name: FieldSymbol, Type: schema.TypeString, Required: true},
			{Name: FieldPrice, Type: schema.TypeFloat, Required: true},
			{Name: FieldVolume, Type: schema.TypeFloat, Required: true},
			{Name: FieldSeq, Type: schema.TypeInt, Required: true},
			{Name: FieldEventTime, Type: schema.TypeTime, Required: true},
		},
	}
}

// MarketEventSchemaV2 在 V1 基础上新增可选字段 exchange（缺省 "UNKNOWN"），
// 是兼容演进的示例。
func MarketEventSchemaV2() schema.Schema {
	s := MarketEventSchemaV1()
	s.Version = 2
	s.Fields = append(s.Fields, schema.Field{
		Name: "exchange", Type: schema.TypeString, Required: false, Default: "UNKNOWN",
	})
	return s
}

// Config 是流水线配置。
type Config struct {
	MaxOutOfOrderness time.Duration // 允许的最大乱序时长
	AllowedLateness   time.Duration // 水位线后仍允许的迟到宽限
	DedupCapacity     int           // 去重表容量
	InputBufferSize   int           // 输入通道容量（背压边界）
	CheckpointEvery   int           // 每处理多少条提交一次断点
	CheckpointPath    string        // 断点文件路径
	CheckpointTTL     time.Duration // 断点有效期（<=0 不过期）
	ResultRingSize    int           // 处理回执环形缓冲容量（观测用）
}

// DefaultConfig 返回本地可用的默认配置。
func DefaultConfig() Config {
	return Config{
		MaxOutOfOrderness: 5 * time.Second,
		AllowedLateness:   1 * time.Second,
		DedupCapacity:     100000,
		InputBufferSize:   1024,
		CheckpointEvery:   500,
		CheckpointPath:    "data/checkpoint.json",
		CheckpointTTL:     24 * time.Hour,
		ResultRingSize:    1024,
	}
}

func (c Config) withDefaults() Config {
	d := DefaultConfig()
	if c.MaxOutOfOrderness != 0 {
		d.MaxOutOfOrderness = c.MaxOutOfOrderness
	}
	if c.AllowedLateness != 0 {
		d.AllowedLateness = c.AllowedLateness
	}
	if c.DedupCapacity != 0 {
		d.DedupCapacity = c.DedupCapacity
	}
	if c.InputBufferSize != 0 {
		d.InputBufferSize = c.InputBufferSize
	}
	if c.CheckpointEvery != 0 {
		d.CheckpointEvery = c.CheckpointEvery
	}
	if c.CheckpointPath != "" {
		d.CheckpointPath = c.CheckpointPath
	}
	if c.CheckpointTTL != 0 {
		d.CheckpointTTL = c.CheckpointTTL
	}
	if c.ResultRingSize != 0 {
		d.ResultRingSize = c.ResultRingSize
	}
	return d
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
// 契约：Emit 必须按事件 ID 幂等。崩溃恢复后已输出但未入断点的事件会被
// 重放，幂等 Sink 去重后保证最终结果不多也不少。
type Sink interface {
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
	cfg        Config
	registry   *schema.Registry
	dedup      *dedup.Store
	tracker    *watermark.Tracker
	cpStore    *checkpoint.Store
	stats      Stats
	results    []event.Result // 环形缓冲，容量 ResultRingSize
	gen        uint64
	lastOffset int64 // 最近处理的一条源位点
}

// New 创建流水线；reg 为结构注册表（至少包含 V1）。
func New(cfg Config, reg *schema.Registry) *Pipeline {
	c := cfg.withDefaults()
	return &Pipeline{
		cfg:      c,
		registry: reg,
		dedup:    dedup.NewStore(c.DedupCapacity),
		tracker:  watermark.NewTracker(c.MaxOutOfOrderness, c.AllowedLateness),
		cpStore:  checkpoint.NewStore(c.CheckpointPath, c.CheckpointTTL),
		results:  make([]event.Result, 0, c.ResultRingSize),
	}
}

// Stats 返回当前统计快照。
func (p *Pipeline) Stats() Stats {
	return p.stats
}

// Results 返回最近的处理回执（环形缓冲，最多 ResultRingSize 条）。
func (p *Pipeline) Results() []event.Result {
	out := make([]event.Result, len(p.results))
	copy(out, p.results)
	return out
}

// Run 从源消费事件直到源耗尽或 ctx 取消。
// 若存在有效断点则自动从断点恢复；断点过期返回明确错误。
func (p *Pipeline) Run(ctx context.Context, src Source, sink Sink) error {
	return p.run(ctx, src, sink, -1)
}

// Resume 显式从断点恢复：要求断点存在且未过期；
// resumeOffset 早于已提交位点时返回 ErrRollback（拒绝回退）。
// resumeOffset < 0 表示使用断点中的已提交位点。
func (p *Pipeline) Resume(ctx context.Context, src Source, sink Sink, resumeOffset int64) error {
	return p.run(ctx, src, sink, resumeOffset)
}

// run 是主流程。resumeOffset < 0 表示自动（有断点用断点，否则从头开始）。
func (p *Pipeline) run(ctx context.Context, src Source, sink Sink, resumeOffset int64) error {
	if _, err := p.restore(src, resumeOffset); err != nil {
		return err
	}

	records := make(chan SourceRecord, p.cfg.InputBufferSize)
	go func() {
		defer close(records)
		for {
			rec, ok := src.Next()
			if !ok {
				return
			}
			select {
			case records <- rec:
			case <-ctx.Done():
				return
			}
		}
	}()

	processed := 0
	for rec := range records {
		if err := ctx.Err(); err != nil {
			return err // 未提交的部分留待恢复时重放
		}
		if err := p.process(rec, sink); err != nil {
			return err
		}
		processed++
		if processed%p.cfg.CheckpointEvery == 0 {
			if err := p.commit(rec.Offset + 1); err != nil {
				return err
			}
		}
	}
	// 源耗尽：强制排空缓冲并提交最终断点。
	for _, ev := range p.tracker.Flush() {
		if err := sink.Emit(ev); err != nil {
			return fmt.Errorf("sink emit: %w", err)
		}
		p.stats.Emitted++
	}
	if processed > 0 {
		if err := p.commit(p.lastOffset + 1); err != nil {
			return err
		}
	}
	return nil
}

// restore 加载断点并恢复内部状态，返回起始位点。
func (p *Pipeline) restore(src Source, resumeOffset int64) (int64, error) {
	st, err := p.cpStore.Load()
	if err != nil {
		return 0, err // 含 ErrExpired：明确拒绝过期断点
	}
	if st == nil {
		if resumeOffset >= 0 {
			return 0, errors.New("no checkpoint to resume from")
		}
		return 0, src.SeekTo(0)
	}
	if resumeOffset >= 0 {
		if err := checkpoint.ValidateResume(st.Offset, resumeOffset); err != nil {
			return 0, err
		}
	}
	p.gen = st.Generation
	p.tracker.SetMaxSeen(st.Watermark)
	buffered := make([]event.MarketEvent, len(st.Buffered))
	for i, b := range st.Buffered {
		buffered[i] = event.MarketEvent{
			ID: b.ID, Symbol: b.Symbol, Price: b.Price,
			Volume: b.Volume, Seq: b.Seq, EventTime: b.EventTime,
		}
	}
	p.tracker.RestoreBuffered(buffered)
	snap := make(map[string]dedup.Entry, len(st.DedupState))
	for k, v := range st.DedupState {
		snap[k] = dedup.Entry{Fingerprint: v.Fingerprint, Seq: v.Seq}
	}
	p.dedup.Restore(snap)
	offset := st.Offset
	if resumeOffset > offset {
		offset = resumeOffset
	}
	return offset, src.SeekTo(offset)
}

// process 处理单条记录：校验 -> 去重 -> 迟到判定 -> 缓冲 -> 输出就绪事件。
func (p *Pipeline) process(rec SourceRecord, sink Sink) error {
	p.lastOffset = rec.Offset

	sch, ok := p.registry.Get(rec.SchemaVersion)
	if !ok {
		p.record(event.Result{Disposition: event.Rejected, Reason: fmt.Sprintf("unknown schema version %d", rec.SchemaVersion)})
		p.stats.Rejected++
		return nil
	}
	norm, err := sch.Normalize(rec.Payload)
	if err != nil {
		p.record(event.Result{Disposition: event.Rejected, Reason: err.Error()})
		p.stats.Rejected++
		return nil
	}
	ev := toMarketEvent(norm)

	switch p.dedup.Check(ev) {
	case dedup.Duplicate:
		p.record(event.Result{EventID: ev.ID, Disposition: event.DuplicateDropped, Reason: "duplicate delivery: identical fingerprint"})
		p.stats.Duplicates++
		return nil
	case dedup.Stale:
		p.record(event.Result{EventID: ev.ID, Disposition: event.StaleDropped, Reason: "stale version: older or equal seq with different content"})
		p.stats.Stales++
		return nil
	case dedup.Correction:
		if p.tracker.Remove(ev.ID) {
			p.tracker.Add(ev)
			p.record(event.Result{EventID: ev.ID, Disposition: event.Merged, Reason: "correction merged: replaced buffered version"})
			p.stats.Merged++
		} else {
			p.record(event.Result{EventID: ev.ID, Disposition: event.LateDropped, Reason: "correction arrived after emit"})
			p.stats.LateDropped++
		}
	case dedup.New:
		if p.tracker.IsLate(ev) {
			p.record(event.Result{EventID: ev.ID, Disposition: event.LateDropped, Reason: "event time beyond allowed lateness"})
			p.stats.LateDropped++
			return nil
		}
		p.tracker.Add(ev)
		p.record(event.Result{EventID: ev.ID, Disposition: event.Accepted, Reason: "buffered for event-time ordering"})
		p.stats.Accepted++
	}

	for _, ready := range p.tracker.DrainReady() {
		if err := sink.Emit(ready); err != nil {
			return fmt.Errorf("sink emit: %w", err)
		}
		p.stats.Emitted++
	}
	return nil
}

// commit 提交断点。
func (p *Pipeline) commit(offset int64) error {
	snap := p.dedup.Snapshot()
	st := checkpoint.State{
		Offset:     offset,
		Watermark:  p.tracker.MaxSeen(),
		DedupState: make(map[string]checkpoint.DedupEntry, len(snap)),
		Generation: p.gen + 1,
	}
	for k, v := range snap {
		st.DedupState[k] = checkpoint.DedupEntry{Fingerprint: v.Fingerprint, Seq: v.Seq}
	}
	for _, ev := range p.tracker.Snapshot() {
		st.Buffered = append(st.Buffered, checkpoint.BufferedEvent{
			ID: ev.ID, Symbol: ev.Symbol, Price: ev.Price,
			Volume: ev.Volume, Seq: ev.Seq, EventTime: ev.EventTime,
		})
	}
	if err := p.cpStore.Save(st); err != nil {
		return fmt.Errorf("commit checkpoint: %w", err)
	}
	p.gen++
	p.stats.Checkpoints++
	return nil
}

// record 追加一条处理回执到环形缓冲。
func (p *Pipeline) record(r event.Result) {
	if len(p.results) < p.cfg.ResultRingSize {
		p.results = append(p.results, r)
		return
	}
	copy(p.results, p.results[1:])
	p.results[len(p.results)-1] = r
}

// toMarketEvent 把清洗后的记录映射为行情事件。
func toMarketEvent(norm map[string]interface{}) event.MarketEvent {
	return event.MarketEvent{
		ID:        norm[FieldID].(string),
		Symbol:    norm[FieldSymbol].(string),
		Price:     norm[FieldPrice].(float64),
		Volume:    norm[FieldVolume].(float64),
		Seq:       uint64(norm[FieldSeq].(int64)),
		EventTime: norm[FieldEventTime].(time.Time),
	}
}
