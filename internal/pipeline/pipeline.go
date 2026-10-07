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
	"sync"
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

// MarketEventSchemaV3 在 V2 基础上再新增一组不同类型的可选字段，用于验证
// 上游新增字段能完整、按稳定缺省语义流转到下游：
//   - venue_category string，缺省 "SPOT"
//   - is_maker       bool，缺省 false
//   - tick_size      float，缺省 0
//   - venue_id       int，缺省 0
func MarketEventSchemaV3() schema.Schema {
	s := MarketEventSchemaV2()
	s.Version = 3
	s.Fields = append(s.Fields,
		schema.Field{Name: "venue_category", Type: schema.TypeString, Required: false, Default: "SPOT"},
		schema.Field{Name: "is_maker", Type: schema.TypeBool, Required: false, Default: false},
		schema.Field{Name: "tick_size", Type: schema.TypeFloat, Required: false, Default: float64(0)},
		schema.Field{Name: "venue_id", Type: schema.TypeInt, Required: false, Default: int64(0)},
	)
	return s
}

// Config 是流水线配置。
type Config struct {
	MaxOutOfOrderness time.Duration // 允许的最大乱序时长
	AllowedLateness   time.Duration // 水位线后仍允许的迟到宽限
	DedupCapacity     int           // 去重表容量
	InputBufferSize   int           // 输入通道容量（背压边界）
	MaxBufferSize     int           // 乱序重排缓冲容量硬上界（<=0 不限）
	CheckpointEvery   int           // 每处理多少条提交一次断点
	CheckpointPath    string        // 断点文件路径（空表示不持久化，进程内一次性运行）
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
		MaxBufferSize:     100000,
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
	if c.MaxBufferSize != 0 {
		d.MaxBufferSize = c.MaxBufferSize
	}
	if c.CheckpointEvery != 0 {
		d.CheckpointEvery = c.CheckpointEvery
	}
	d.CheckpointPath = c.CheckpointPath
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
	Line          int // 源中的行号（1 起），用于错误回执定位
	SchemaVersion int
	Payload       map[string]interface{}
	// ParseError 非空表示该行无法解析为合法记录；流水线将其计为 REJECTED。
	ParseError string
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

// DurableSink 是支持断点协同的持久化 Sink（可选实现）。
// 流水线在提交断点前先调用 Sync 确保已 Emit 的数据落盘，再读取
// DurableOffset 作为该次断点的 sink 位点；恢复时用 Truncate 把输出
// 回滚到该位点，未持久化的重放部分被丢弃，从而保证输出恰好一次。
type DurableSink interface {
	Sink
	// Sync 强制此前 Emit 的数据持久化。
	Sync() error
	// DurableOffset 返回已持久化输出对应的字节位点。
	DurableOffset() int64
	// Truncate 将输出截断到断点记录的字节位点（用于恢复回滚）。
	Truncate(offset int64) error
}

// Stats 汇总运行期统计，供测试与观测断言。
type Stats struct {
	Processed     int // 读取并处置的输入总行数（含被拒绝行）
	Accepted      int
	Duplicates    int
	Stales        int
	Merged        int
	LateDropped   int
	Rejected      int
	Emitted       int // 实际交给 sink 的事件数（含可能的重放）
	ForcedEmitted int // 因乱序缓冲达到上界而被强制提前输出的事件数
	BufferedMax   int // 运行期乱序缓冲峰值
	Checkpoints   int
}

// cpStore 抽象断点存储，便于在不持久化（CheckpointPath 为空）时使用进程内实现。
type cpStore interface {
	Load() (*checkpoint.State, error)
	Save(st checkpoint.State) error
}

// memCPStore 是不做持久化的进程内断点存储（一次性运行 / stdin 场景）。
type memCPStore struct{ st *checkpoint.State }

func (m *memCPStore) Load() (*checkpoint.State, error) { return m.st, nil }
func (m *memCPStore) Save(st checkpoint.State) error   { cp := st; m.st = &cp; return nil }

// Pipeline 是行情事件处理流水线。
type Pipeline struct {
	cfg        Config
	registry   *schema.Registry
	dedup      *dedup.Store
	tracker    *watermark.Tracker
	cpStore    cpStore
	stats      Stats
	results    []event.Result // 环形缓冲，容量 ResultRingSize
	gen        uint64
	lastOffset int64 // 最近处理的一条源位点
}

// New 创建流水线；reg 为结构注册表（至少包含 V1）。
// cfg.CheckpointPath 为空时不做断点持久化，适用于 stdin/stdout 一次性运行。
func New(cfg Config, reg *schema.Registry) *Pipeline {
	c := cfg.withDefaults()
	var store cpStore
	if c.CheckpointPath == "" {
		store = &memCPStore{}
	} else {
		store = checkpoint.NewStore(c.CheckpointPath, c.CheckpointTTL)
	}
	return &Pipeline{
		cfg:      c,
		registry: reg,
		dedup:    dedup.NewStore(c.DedupCapacity),
		tracker:  watermark.NewTracker(c.MaxOutOfOrderness, c.AllowedLateness, c.MaxBufferSize),
		cpStore:  store,
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
	if _, err := p.restore(src, sink, resumeOffset); err != nil {
		return err
	}

	records := make(chan SourceRecord, p.cfg.InputBufferSize)
	var producers sync.WaitGroup
	producers.Add(1)
	go func() {
		defer producers.Done()
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

	var runErr error
	processed := 0
loop:
	for {
		select {
		case <-ctx.Done():
			runErr = ctx.Err() // 未提交的部分留待恢复时重放
			break loop
		case rec, ok := <-records:
			if !ok {
				break loop
			}
			// 取出记录后优先复查取消：records 有缓冲，取消后其中可能仍有
			// 数据，select 可能继续命中 records；这里保证取消优先于继续处理。
			if err := ctx.Err(); err != nil {
				runErr = err
				break loop
			}
			if err := p.process(ctx, rec, sink); err != nil {
				runErr = err
				break loop
			}
			processed++
			if p.cfg.CheckpointEvery > 0 && processed%p.cfg.CheckpointEvery == 0 {
				if err := p.commit(rec.Offset+1, sink); err != nil {
					runErr = err
					break loop
				}
			}
		}
	}
	// 等待生产者退出，避免在 ctx 取消时泄漏阻塞在发送上的 goroutine。
	producers.Wait()
	if runErr != nil {
		return runErr
	}
	// 源耗尽：强制排空缓冲并提交最终断点。
	for _, ev := range p.tracker.Flush() {
		if err := p.emit(ctx, sink, ev, false); err != nil {
			return err
		}
	}
	if processed > 0 {
		if err := p.commit(p.lastOffset+1, sink); err != nil {
			return err
		}
	}
	return nil
}

// emit 向下游输出一条事件并更新统计。
// 输出前检查 ctx，使取消能在同一轮的连续 Emit 之间即时生效。
func (p *Pipeline) emit(ctx context.Context, sink Sink, ev event.MarketEvent, forced bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := sink.Emit(ev); err != nil {
		return fmt.Errorf("sink emit: %w", err)
	}
	p.stats.Emitted++
	if forced {
		p.stats.ForcedEmitted++
	}
	return nil
}

// restore 加载断点并恢复内部状态，返回起始位点。
func (p *Pipeline) restore(src Source, sink Sink, resumeOffset int64) (int64, error) {
	st, err := p.cpStore.Load()
	if err != nil {
		return 0, err // 含 ErrExpired：明确拒绝过期断点
	}
	if st == nil {
		if resumeOffset >= 0 {
			return 0, errors.New("no checkpoint to resume from")
		}
		// 全新开始：持久化输出清空到 0，避免混入上一次运行的残留。
		if ds, ok := sink.(DurableSink); ok {
			if err := ds.Truncate(0); err != nil {
				return 0, fmt.Errorf("reset sink: %w", err)
			}
		}
		return 0, src.SeekTo(0)
	}
	if resumeOffset >= 0 {
		if err := checkpoint.ValidateResume(st.Offset, resumeOffset); err != nil {
			return 0, err
		}
	}
	// 先把持久化输出截断到断点位点，再重放未落盘部分（若有），
	// 保证最终输出与无崩溃运行逐字节一致（恰好一次）。
	if ds, ok := sink.(DurableSink); ok {
		if err := ds.Truncate(st.SinkOffset); err != nil {
			return 0, fmt.Errorf("truncate sink to checkpoint: %w", err)
		}
	}
	p.gen = st.Generation
	p.tracker.SetMaxSeen(st.Watermark)
	p.tracker.SetEmittedFloor(st.EmittedFloor, st.HasFloor)
	buffered := make([]event.MarketEvent, len(st.Buffered))
	for i, b := range st.Buffered {
		bev := event.MarketEvent{
			ID: b.ID, Symbol: b.Symbol, Price: b.Price,
			Volume: b.Volume, Seq: b.Seq, EventTime: b.EventTime,
			Attrs: copyAttrs(b.Attrs),
		}
		bev.SetSchemaVersion(b.SchemaVersion)
		buffered[i] = bev
	}
	p.tracker.RestoreBuffered(buffered)
	if n := p.tracker.Buffered(); n > p.stats.BufferedMax {
		p.stats.BufferedMax = n
	}
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

// copyAttrs 复制扩展字段 map，避免缓冲/断点之间共享底层 map。
func copyAttrs(in map[string]interface{}) map[string]interface{} {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]interface{}, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// process 处理单条记录：校验 -> 去重 -> 迟到判定 -> 缓冲 -> 输出就绪事件。
func (p *Pipeline) process(ctx context.Context, rec SourceRecord, sink Sink) error {
	p.lastOffset = rec.Offset
	p.stats.Processed++

	if rec.ParseError != "" {
		p.record(event.Result{Disposition: event.Rejected,
			Reason: fmt.Sprintf("line %d: malformed record: %s", rec.Line, rec.ParseError)})
		p.stats.Rejected++
		return nil
	}

	sch, ok := p.registry.Get(rec.SchemaVersion)
	if !ok {
		p.record(event.Result{Disposition: event.Rejected,
			Reason: fmt.Sprintf("line %d: unknown schema version %d", rec.Line, rec.SchemaVersion)})
		p.stats.Rejected++
		return nil
	}
	norm, err := sch.Normalize(rec.Payload)
	if err != nil {
		p.record(event.Result{Disposition: event.Rejected,
			Reason: fmt.Sprintf("line %d: %s", rec.Line, err.Error())})
		p.stats.Rejected++
		return nil
	}
	ev := toMarketEvent(rec.SchemaVersion, norm)

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
			if err := p.addBuffered(ctx, sink, ev); err != nil {
				return err
			}
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
		if err := p.addBuffered(ctx, sink, ev); err != nil {
			return err
		}
		p.record(event.Result{EventID: ev.ID, Disposition: event.Accepted, Reason: "buffered for event-time ordering"})
		p.stats.Accepted++
	}

	for _, ready := range p.tracker.DrainReady() {
		if err := p.emit(ctx, sink, ready, false); err != nil {
			return err
		}
	}
	return nil
}

// addBuffered 将事件放入乱序缓冲；若因容量上界触发强制输出则立即发给 sink。
// 强制输出先于同轮 DrainReady 发生，且其事件时间不大于水位线就绪事件，
// 因而输出序列保持事件时间非降序。
func (p *Pipeline) addBuffered(ctx context.Context, sink Sink, ev event.MarketEvent) error {
	forced, forcedOK := p.tracker.Add(ev)
	if n := p.tracker.Buffered(); n > p.stats.BufferedMax {
		p.stats.BufferedMax = n
	}
	if forcedOK {
		if err := p.emit(ctx, sink, forced, true); err != nil {
			return err
		}
	}
	return nil
}

// commit 提交断点。若 sink 是 DurableSink，先 Sync 再记录其持久化位点。
func (p *Pipeline) commit(offset int64, sink Sink) error {
	var sinkOffset int64
	if ds, ok := sink.(DurableSink); ok {
		if err := ds.Sync(); err != nil {
			return fmt.Errorf("sync sink: %w", err)
		}
		sinkOffset = ds.DurableOffset()
	}
	snap := p.dedup.Snapshot()
	st := checkpoint.State{
		Offset:     offset,
		SinkOffset: sinkOffset,
		Watermark:  p.tracker.MaxSeen(),
		DedupState: make(map[string]checkpoint.DedupEntry, len(snap)),
		Generation: p.gen + 1,
	}
	if floor, ok := p.tracker.EmittedFloor(); ok {
		st.EmittedFloor = floor
		st.HasFloor = true
	}
	for k, v := range snap {
		st.DedupState[k] = checkpoint.DedupEntry{Fingerprint: v.Fingerprint, Seq: v.Seq}
	}
	for _, ev := range p.tracker.Snapshot() {
		st.Buffered = append(st.Buffered, checkpoint.BufferedEvent{
			SchemaVersion: ev.SchemaVersion(),
			ID:            ev.ID, Symbol: ev.Symbol, Price: ev.Price,
			Volume: ev.Volume, Seq: ev.Seq, EventTime: ev.EventTime,
			Attrs: copyAttrs(ev.Attrs),
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

// coreFields 是行情事件的核心字段集合，其余已声明字段一律进入 Attrs。
var coreFields = map[string]struct{}{
	FieldID:        {},
	FieldSymbol:    {},
	FieldPrice:     {},
	FieldVolume:    {},
	FieldSeq:       {},
	FieldEventTime: {},
}

// toMarketEvent 把清洗后的记录映射为行情事件：核心字段落到强类型字段，
// 该结构版本声明的其余字段（含按缺省值补齐的新字段）完整进入 Attrs，
// 一路透传到最终输出；结构未声明的字段在 Normalize 阶段已被忽略。
func toMarketEvent(schemaVersion int, norm map[string]interface{}) event.MarketEvent {
	var attrs map[string]interface{}
	for k, v := range norm {
		if _, isCore := coreFields[k]; isCore {
			continue
		}
		if attrs == nil {
			attrs = make(map[string]interface{})
		}
		attrs[k] = v
	}
	ev := event.MarketEvent{
		ID:        norm[FieldID].(string),
		Symbol:    norm[FieldSymbol].(string),
		Price:     norm[FieldPrice].(float64),
		Volume:    norm[FieldVolume].(float64),
		Seq:       uint64(norm[FieldSeq].(int64)),
		EventTime: norm[FieldEventTime].(time.Time),
		Attrs:     attrs,
	}
	ev.SetSchemaVersion(schemaVersion)
	return ev
}
