// Package pipeline 把 ingress、schema、事件时间处理、checkpoint、sink 串成可恢复的处理流。
//
// 并发模型（背压防 OOM）：
//   - 一个读取协程把原始行送入有界通道；通道满则读取自然阻塞，不会无限堆积；
//   - 一个处理协程串行维护 watermark / 去重表 / 排序缓冲（状态因此无需加锁）；
//   - 排序缓冲达到上限时，处理循环触发墙钟水位推进以尽快到点输出；仍满则等待而非丢弃；
//   - sink 同步确认后才推进 checkpoint（崩溃恢复恰好一次）。
package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/feng111627/market-event-pipeline-go/internal/checkpoint"
	"github.com/feng111627/market-event-pipeline-go/internal/event"
	"github.com/feng111627/market-event-pipeline-go/internal/schema"
	"github.com/feng111627/market-event-pipeline-go/internal/sink"
)

// Config 管线参数。
type Config struct {
	AllowedLateness   time.Duration // 乱序容忍窗口（watermark = maxObserved - AllowedLateness）
	BufferLimit       int           // 排序缓冲条数上限（背压阈值）
	InputQueue        int           // 读取->处理 有界队列长度
	CheckpointEvery   time.Duration // 周期断点间隔（<=0 表示仅在每条记录处理后提交）
	CheckpointTTL     time.Duration // 断点有效期（<=0 不做时间过期检查）
	Epoch             int64         // 本次启动纪元（与断点中不一致则拒绝恢复）
	WatermarkInterval time.Duration // 无显式水位输入时墙钟推进的最小间隔

	// FlushOnEOF 控制 EOF 是否冲刷在途记录：零值=冲刷（正常默认）；
	// 显式设为 FlushOnEOFOff 则不冲刷，只提交断点（用于精确模拟“进程被杀”的崩溃语义）。
	FlushOnEOF int
}

// EOF 冲刷开关取值。
const (
	FlushOnEOFOn  = 0 // 默认：EOF 冲刷全部在途记录
	FlushOnEOFOff = 1 // 崩溃模拟：EOF 不冲刷，pending 留在断点中
)

// Stats 运行统计；每种处理原因独立计数，测试日志可据此判断业务覆盖情况。
type Stats struct {
	Accepted         int64 `json:"accepted"`
	Merged           int64 `json:"merged"`
	Buffered         int64 `json:"buffered"`
	DroppedLate      int64 `json:"dropped_late"`
	Duplicate        int64 `json:"duplicate"`
	ConflictRejected int64 `json:"conflict_rejected"`
	SchemaRejected   int64 `json:"schema_rejected"`
	Backpressured    int64 `json:"backpressured"`
	WatermarkCtrl    int64 `json:"watermark_ctrl"`
	ControlRejected  int64 `json:"control_rejected"`
}

// Pipeline 行情事件处理管线。
type Pipeline struct {
	cfg      Config
	registry *schema.Registry
	sink     sink.Sink
	cpStore  checkpoint.Store
	clock    event.Clock
	logger   *log.Logger
	stats    Stats
	proc     *Processor

	committedOffset int64 // 最近一次已提交到 sink 的输入偏移
	committedWM     int64 // 最近一次已提交水位
	lastCP          time.Time
	flushOnEOF      bool
}

// ErrBackpressure 内部背压信号（测试可断言统计）。
var ErrBackpressure = errors.New("pipeline backpressure")

// New 构造管线。
func New(cfg Config, reg *schema.Registry, sk sink.Sink, cp checkpoint.Store, clock event.Clock, logger *log.Logger) *Pipeline {
	if logger == nil {
		logger = log.Default()
	}
	if cfg.InputQueue <= 0 {
		cfg.InputQueue = 128
	}
	if cfg.BufferLimit <= 0 {
		cfg.BufferLimit = 10000
	}
	return &Pipeline{
		cfg:        cfg,
		registry:   reg,
		sink:       sk,
		cpStore:    cp,
		clock:      clock,
		logger:     logger,
		flushOnEOF: cfg.FlushOnEOF == FlushOnEOFOn,
		proc:       NewProcessor(cfg.AllowedLateness),
	}
}

// Restore 从断点恢复处理器内部状态。调用方应先用 checkpoint.Validator 校验。
func (p *Pipeline) Restore(s *checkpoint.State) error {
	p.committedOffset = s.InputOffset
	p.committedWM = s.WatermarkUnix
	return p.proc.Restore(s.Processor)
}

// Stats 返回当前统计快照。
func (p *Pipeline) Stats() Stats { return p.stats }

// LineReader 管线需要的最小输入接口。
type LineReader interface {
	Next() (event.Source, error)
	Offset() int64
}

type queuedLine struct {
	src       event.Source
	endOffset int64 // 读取该行后 reader 的已消费字节位置（下一行起点，含换行）
	err       error
}

// Run 消费输入流直到 EOF 或 ctx 取消。调用前若存在已校验断点，应先调用 Restore。
func (p *Pipeline) Run(ctx context.Context, r LineReader) (Stats, error) {
	queue := make(chan queuedLine, p.cfg.InputQueue)

	// 读取协程：通道满时阻塞 => 突发流量下读取侧背压，不会撑爆内存。
	go func() {
		defer close(queue)
		for {
			src, err := r.Next()
			end := r.Offset()
			select {
			case queue <- queuedLine{src: src, endOffset: end, err: err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()

	wmTicker := newTicker(p.cfg.WatermarkInterval)
	defer wmTicker.Stop()
	cpTicker := newTicker(p.cfg.CheckpointEvery)
	defer cpTicker.Stop()

	var runErr error
loop:
	for {
		select {
		case <-ctx.Done():
			runErr = ctx.Err()
			break loop
		case <-cpTicker.C:
			if err := p.checkpoint(ctx); err != nil {
				runErr = err
				break loop
			}
		case <-wmTicker.C:
			p.advanceWallWatermark(ctx)
		case ql, ok := <-queue:
			if !ok {
				break loop
			}
			if ql.err != nil {
				if errors.Is(ql.err, io.EOF) {
					break loop
				}
				runErr = ql.err
				break loop
			}
			if err := p.handleLine(ctx, ql.src, ql.endOffset); err != nil {
				runErr = err
				break loop
			}
		}
	}

	// 收尾：正常 EOF 冲刷在途记录；崩溃模式（flushOnEOF=false）只提交断点，保留 pending。
	if p.flushOnEOF {
		p.flushAllAndEmit(ctx)
	}
	if cpErr := p.checkpoint(ctx); cpErr != nil && runErr == nil {
		runErr = cpErr
	}
	return p.stats, runErr
}

// handleLine 分类并处理一行：控制记录 / 行情记录（含背压等待重试）。
func (p *Pipeline) handleLine(ctx context.Context, src event.Source, endOffset int64) error {
	if wm, isCtrl := parseWatermarkControl(src.Line); isCtrl {
		emitted, err := p.proc.AdvanceWatermark(wm)
		if err != nil {
			p.stats.ControlRejected++
			p.logger.Printf("decision reason=%s offset=%d detail=%v", event.ReasonControlRejected, endOffset, err)
			p.proc.inOffset = endOffset
			return p.checkpointAfter(ctx, endOffset)
		}
		p.stats.WatermarkCtrl++
		p.logger.Printf("decision reason=%s offset=%d watermark=%s", event.ReasonControlWatermark, endOffset, wm.Format(time.RFC3339Nano))
		if err := p.emitAll(ctx, emitted); err != nil {
			return err
		}
		return p.checkpointAfter(ctx, endOffset)
	}

	rec, derr := p.registry.Decode(src.Line)
	if derr != nil {
		// 不兼容/非法结构：明确拒绝并作为一条终态结论落盘，绝不让错误语义流向下游。
		p.stats.SchemaRejected++
		p.logger.Printf("decision reason=%s offset=%d detail=%v", event.ReasonSchemaRejected, endOffset, derr)
		p.proc.inOffset = endOffset
		return p.emitTerminal(ctx, sink.Outcome{
			Reason: event.ReasonSchemaRejected,
			Detail: derr.Error(),
			SeqID:  extractSeqIDHint(src.Line),
		}, endOffset)
	}

	// 背压：缓冲已满。为保证内存有界且不丢数据，强制把最旧在途记录提前到点输出
	// （牺牲部分乱序等待），并以 backpressure 原因记录这次腾挪；绝不静默丢弃。
	if p.proc.BufferLen() >= p.cfg.BufferLimit {
		p.stats.Backpressured++
		p.logger.Printf("decision reason=%s offset=%d buffer=%d limit=%d: force-flushing oldest buffered events",
			event.ReasonBackpressure, endOffset, p.proc.BufferLen(), p.cfg.BufferLimit)
		forced := p.proc.ForceFlushOldest()
		if err := p.emitAll(ctx, forced); err != nil {
			return err
		}
	}

	dec, emitted := p.proc.Ingest(rec, endOffset, p.clock.Now())
	switch dec.Reason {
	case event.ReasonLateDrop:
		p.stats.DroppedLate++
		p.logger.Printf("decision reason=%s offset=%d seq_id=%s detail=%s", dec.Reason, endOffset, rec.SeqID, dec.Detail)
		return p.emitTerminal(ctx, outcomeFromEvent(p.proc.allocSeq(), dec.Reason, dec.Detail, dec.Event), endOffset)
	case event.ReasonDuplicate:
		p.stats.Duplicate++
		p.logger.Printf("decision reason=%s offset=%d seq_id=%s detail=%s", dec.Reason, endOffset, rec.SeqID, dec.Detail)
		// 重复不计入结果，但输入偏移仍推进并提交。
		p.proc.inOffset = endOffset
		return p.checkpointAfter(ctx, endOffset)
	case event.ReasonConflict:
		p.stats.ConflictRejected++
		p.logger.Printf("decision reason=%s offset=%d seq_id=%s detail=%s", dec.Reason, endOffset, rec.SeqID, dec.Detail)
		return p.emitTerminal(ctx, outcomeFromEvent(p.proc.allocSeq(), dec.Reason, dec.Detail, dec.Event), endOffset)
	case event.ReasonMerged:
		p.stats.Merged++
		p.logger.Printf("decision reason=%s offset=%d seq_id=%s detail=%s", dec.Reason, endOffset, rec.SeqID, dec.Detail)
		p.proc.inOffset = endOffset
		return p.checkpointAfter(ctx, endOffset)
	default:
		p.stats.Buffered++
		p.logger.Printf("decision reason=%s offset=%d seq_id=%s detail=%s", dec.Reason, endOffset, rec.SeqID, dec.Detail)
	}
	if err := p.emitAll(ctx, emitted); err != nil {
		return err
	}
	return p.checkpointAfter(ctx, endOffset)
}

// emitAll 把处理器到点输出写入 sink 并计数。
func (p *Pipeline) emitAll(ctx context.Context, emitted []emitted) error {
	for _, e := range emitted {
		o := outcomeFromEvent(e.Seq, event.ReasonAccepted, e.Detail, &e.Ev)
		if e.Merged {
			o.Detail = e.Detail
		}
		if err := p.emitOne(ctx, o, e.Offset); err != nil {
			return err
		}
		p.stats.Accepted++
	}
	return nil
}

// emitTerminal 输出一条终态但非行情结果的结论（schema 拒绝/冲突/晚到）。
func (p *Pipeline) emitTerminal(ctx context.Context, o sink.Outcome, offset int64) error {
	return p.emitOne(ctx, o, offset)
}

// emitOne 写入 sink 并把“输入偏移/水位”推进到该结论对应的提交点。
func (p *Pipeline) emitOne(ctx context.Context, o sink.Outcome, offset int64) error {
	if err := p.sink.Emit(ctx, o); err != nil {
		// 恢复重放命中已确认序号：属于重复提交，不应当作失败（不多计）。
		if errors.Is(err, sink.ErrAlreadyAcked) {
			p.logger.Printf("sink dedup seq=%d already acknowledged, skip", o.Seq)
			return nil
		}
		return err
	}
	p.committedOffset = offset
	if p.proc.wm > p.committedWM {
		p.committedWM = p.proc.wm
	}
	return nil
}

// checkpointAfter 对非输出类结论（重复/合并/控制记录）也推进提交偏移。
func (p *Pipeline) checkpointAfter(ctx context.Context, offset int64) error {
	if offset > p.committedOffset {
		p.committedOffset = offset
	}
	if p.proc.wm > p.committedWM {
		p.committedWM = p.proc.wm
	}
	if p.cfg.CheckpointEvery <= 0 {
		return p.checkpoint(ctx)
	}
	return nil
}

// flushAllAndEmit EOF/停机时冲刷全部在途记录。
func (p *Pipeline) flushAllAndEmit(ctx context.Context) {
	emitted := p.proc.FlushAll()
	if err := p.emitAll(ctx, emitted); err != nil {
		p.logger.Printf("flush emit error: %v", err)
	}
}

// advanceWallWatermark 依据已观测最大事件时间与容忍窗口推进水位。
// 仅当已经由显式 watermark 建立过基准（seeded）时才允许墙钟推进，
// 避免在没有任何水位输入时凭空把未来时间戳的记录提前输出。
func (p *Pipeline) advanceWallWatermark(ctx context.Context) {
	if p.proc.maxObs == 0 || p.proc.wm == 0 {
		return
	}
	wm := time.Unix(0, p.proc.maxObs).Add(-p.cfg.AllowedLateness)
	if wm.UnixNano() <= p.proc.wm {
		return
	}
	emitted, err := p.proc.AdvanceWatermark(wm)
	if err != nil {
		return
	}
	if err := p.emitAll(ctx, emitted); err != nil {
		p.logger.Printf("watermark emit error: %v", err)
	}
}

// checkpoint 持久化当前断点（sink 已确认序号 + 处理器在途状态）。
func (p *Pipeline) checkpoint(ctx context.Context) error {
	if p.cpStore == nil {
		return nil
	}
	raw, err := p.proc.Snapshot()
	if err != nil {
		return err
	}
	s := checkpoint.State{
		Version:       1,
		Epoch:         p.cfg.Epoch,
		InputOffset:   p.committedOffset,
		WatermarkUnix: p.committedWM,
		SinkSeq:       p.sink.AcknowledgedSeq(),
		UpdatedAt:     p.clock.Now(),
		Processor:     raw,
	}
	if err := p.cpStore.Save(ctx, s); err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}
	return nil
}

// parseWatermarkControl 识别输入中的水位控制记录：
//
//	{"type":"watermark","watermark":"2026-10-06T10:00:05Z"}
func parseWatermarkControl(line string) (time.Time, bool) {
	var head map[string]json.RawMessage
	if err := json.Unmarshal([]byte(line), &head); err != nil {
		return time.Time{}, false
	}
	raw, ok := head["type"]
	if !ok {
		return time.Time{}, false
	}
	var typ string
	if err := json.Unmarshal(raw, &typ); err != nil || typ != "watermark" {
		return time.Time{}, false
	}
	var wmStr string
	if err := json.Unmarshal(head["watermark"], &wmStr); err != nil {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339Nano, wmStr)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// extractSeqIDHint 尽力从非法记录中取出 seq_id 用于审计落盘。
func extractSeqIDHint(line string) string {
	var head struct {
		SeqID string `json:"seq_id"`
	}
	_ = json.Unmarshal([]byte(line), &head)
	return head.SeqID
}

func outcomeFromEvent(seq int64, reason event.Reason, detail string, ev *event.MarketEvent) sink.Outcome {
	o := sink.Outcome{
		Seq: seq, Reason: reason, Detail: detail,
		EventTime: ev.EventTime, Symbol: ev.Symbol, SeqID: ev.SeqID,
		Price: ev.Price, Qty: ev.Qty, SchemaV: ev.SchemaV, Extra: ev.Extra,
	}
	return o
}

// newTicker 包装 time.Ticker，间隔<=0 时返回一个永不触发的 ticker。
func newTicker(d time.Duration) *time.Ticker {
	if d <= 0 {
		// 用极大间隔模拟“不周期触发”，避免空 channel 改造 select。
		return time.NewTicker(24 * time.Hour)
	}
	return time.NewTicker(d)
}
