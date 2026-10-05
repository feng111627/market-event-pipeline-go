package pipeline

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/feng111627/market-event-pipeline-go/internal/event"
	"github.com/feng111627/market-event-pipeline-go/internal/schema"
)

// pendingRec 是“已接收但尚未到事件时间输出点”的记录。
type pendingRec struct {
	Ev     event.MarketEvent
	Offset int64 // 该事件来源输入行的起始偏移（用于提交点）
	SeenBy map[string]bool
}

// seenRec 是去重表条目。
type seenRec struct {
	Fingerprint string
	ET          int64 // unix nano，用于 watermark 推进后清理
}

// emitted 描述一次到点输出。
type emitted struct {
	Ev     event.MarketEvent
	Offset int64
	Seq    int64
	Merged bool
	Detail string
}

// Processor 串行维护 watermark、去重表与按事件时间的排序缓冲。
// 本类型非并发安全，设计上只由管线的单一处理协程使用。
type Processor struct {
	lateness time.Duration
	maxObs   int64 // 已接收记录中最大的 event_time（unix nano）
	wm       int64 // 当前 watermark（unix nano）
	pending  []*pendingRec
	seen     map[string]seenRec
	outSeq   int64 // 已产出的 sink 序号
	inOffset int64 // 已处理到的输入偏移（该偏移之前的行都已给出最终结论或已缓冲）
}

// NewProcessor 创建处理器；lateness 为乱序容忍窗口。
func NewProcessor(lateness time.Duration) *Processor {
	return &Processor{lateness: lateness, seen: make(map[string]seenRec)}
}

func (p *Processor) watermarkTime() time.Time { return time.Unix(0, p.wm) }

// BufferLen 当前排序缓冲条数。
func (p *Processor) BufferLen() int { return len(p.pending) }

// allocSeq 分配下一个全局单调输出序号（accepted 与各类终态结论共用一个序号空间）。
func (p *Processor) allocSeq() int64 { p.outSeq++; return p.outSeq }

// snapshotData / loadData 用于断点持久化。
type snapshotData struct {
	MaxObs    int64              `json:"max_obs"`
	Watermark int64              `json:"watermark"`
	Pending   []*pendingRec      `json:"pending"`
	Seen      map[string]seenRec `json:"seen"`
	OutSeq    int64              `json:"out_seq"`
	InOffset  int64              `json:"in_offset"`
}

// Snapshot 序列化处理器内部状态。
func (p *Processor) Snapshot() (json.RawMessage, error) {
	return json.Marshal(snapshotData{
		MaxObs: p.maxObs, Watermark: p.wm, Pending: p.pending,
		Seen: p.seen, OutSeq: p.outSeq, InOffset: p.inOffset,
	})
}

// Restore 从断点恢复处理器内部状态。
func (p *Processor) Restore(raw json.RawMessage) error {
	var d snapshotData
	if err := json.Unmarshal(raw, &d); err != nil {
		return fmt.Errorf("restore processor: %w", err)
	}
	p.maxObs, p.wm = d.MaxObs, d.Watermark
	p.pending = d.Pending
	if p.pending == nil {
		p.pending = nil
	}
	p.seen = d.Seen
	if p.seen == nil {
		p.seen = make(map[string]seenRec)
	}
	p.outSeq, p.inOffset = d.OutSeq, d.InOffset
	return nil
}

// fingerprint 计算记录内容指纹（seq_id 之外的业务语义）。
// 相同 seq_id + 相同指纹 => 重复投递；相同 seq_id + 不同指纹 => 冲突，拒绝。
func fingerprint(ev *event.MarketEvent) string {
	keys := make([]string, 0, len(ev.Extra))
	for k := range ev.Extra {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	h := sha256.New()
	fmt.Fprintf(h, "%s|%d|%d|%d|%d", ev.Symbol, ev.EventTime.UnixNano(), ev.Price, ev.Qty, ev.SchemaV)
	for _, k := range keys {
		fmt.Fprintf(h, "|%s=%d", k, ev.Extra[k])
	}
	return hex.EncodeToString(h.Sum(nil))
}

// Ingest 处理一条已通过 schema 校验的记录，返回决策与可能产生的输出。
// emit 非空表示有记录到点输出（accepted/merged）；d.Reason 描述“本条输入”的结论。
func (p *Processor) Ingest(d *schema.DecodedRecord, offset int64, recv time.Time) (dec event.Decision, emit []emitted) {
	p.inOffset = offset
	ev := event.MarketEvent{
		SchemaV: d.SchemaV, SeqID: d.SeqID, Symbol: d.Symbol,
		Price: d.Price, Qty: d.Qty, EventTime: d.EventTime,
		RecvTime: recv, Extra: d.Fields,
	}
	et := ev.EventTime.UnixNano()
	fp := fingerprint(&ev)

	// 1) 幂等判定优先于晚到判定：
	//    已投递过的记录即使在其事件时间过期后重投，也必须记为“重复”而不是“晚到”，
	//    结论要能区分“重投”与“另一条真正的迟到数据”。
	if old, ok := p.seen[ev.SeqID]; ok {
		if old.Fingerprint == fp {
			return event.Decision{
				Reason: event.ReasonDuplicate,
				Event:  &ev,
				Detail: "seq_id=" + ev.SeqID + " already seen with identical content; not counted again",
			}, nil
		}
		return event.Decision{
			Reason: event.ReasonConflict,
			Event:  &ev,
			Detail: fmt.Sprintf("seq_id=%s reused with different content (fp %s vs %s)", ev.SeqID, old.Fingerprint[:12], fp[:12]),
		}, nil
	}

	// 2) 晚到：未见过的新事件，event_time < watermark，明确丢弃（不静默）
	if p.wm > 0 && et < p.wm {
		return event.Decision{
			Reason: event.ReasonLateDrop,
			Event:  &ev,
			Detail: fmt.Sprintf("event_time=%s < watermark=%s", ev.EventTime.Format(time.RFC3339Nano), p.watermarkTime().Format(time.RFC3339Nano)),
		}, nil
	}

	// 3) 与同 symbol+event_time 的在途记录合并（部分字段到达）
	for _, q := range p.pending {
		if q.Ev.Symbol == ev.Symbol && q.Ev.EventTime.Equal(ev.EventTime) {
			merged, conflict := mergeInto(q, &ev)
			if conflict != "" {
				return event.Decision{
					Reason: event.ReasonConflict,
					Event:  &ev,
					Detail: conflict,
				}, nil
			}
			p.seen[ev.SeqID] = seenRec{Fingerprint: fp, ET: et}
			if et > p.maxObs {
				p.maxObs = et
			}
			return event.Decision{
				Reason: event.ReasonMerged,
				Event:  &q.Ev,
				Detail: "merged into seq_id=" + q.Ev.SeqID + ": " + strings.Join(merged, ","),
			}, nil
		}
	}

	// 4) 进入排序缓冲，等待事件时间到点
	p.pending = append(p.pending, &pendingRec{Ev: ev, Offset: offset, SeenBy: map[string]bool{ev.SeqID: true}})
	p.seen[ev.SeqID] = seenRec{Fingerprint: fp, ET: et}
	if et > p.maxObs {
		p.maxObs = et
	}

	out := p.flushReady()
	return event.Decision{
		Reason: event.ReasonBuffered,
		Event:  &ev,
		Detail: fmt.Sprintf("buffered %d record(s), waiting for event_time", len(p.pending)),
	}, out
}

// mergeInto 把 src 合并进 dst（first-non-zero wins）。
// 返回“被补全的字段列表”；若同一字段两边都有值且不一致，则返回冲突描述。
func mergeInto(dst *pendingRec, src *event.MarketEvent) (mergedFields []string, conflict string) {
	if dst.Ev.Price != 0 && src.Price != 0 && dst.Ev.Price != src.Price {
		return nil, fmt.Sprintf("merge conflict on price for %s@%s: %d vs %d",
			src.Symbol, src.EventTime.Format(time.RFC3339Nano), dst.Ev.Price, src.Price)
	}
	if dst.Ev.Qty != 0 && src.Qty != 0 && dst.Ev.Qty != src.Qty {
		return nil, fmt.Sprintf("merge conflict on qty for %s@%s: %d vs %d",
			src.Symbol, src.EventTime.Format(time.RFC3339Nano), dst.Ev.Qty, src.Qty)
	}
	if dst.Ev.Price == 0 && src.Price != 0 {
		dst.Ev.Price = src.Price
		mergedFields = append(mergedFields, "price")
	}
	if dst.Ev.Qty == 0 && src.Qty != 0 {
		dst.Ev.Qty = src.Qty
		mergedFields = append(mergedFields, "qty")
	}
	// Extra：缺省物化字段在“首次非缺省值出现”时视为可补全
	for k, v := range src.Extra {
		dv := dst.Ev.Extra[k]
		if dv == v {
			continue
		}
		if dv == 0 && v != 0 {
			dst.Ev.Extra[k] = v
			mergedFields = append(mergedFields, "extra."+k)
			continue
		}
		if dv != 0 && v != 0 {
			return nil, fmt.Sprintf("merge conflict on extra.%s for %s@%s: %d vs %d",
				k, src.Symbol, src.EventTime.Format(time.RFC3339Nano), dv, v)
		}
	}
	dst.Ev.SchemaV = src.SchemaV // 合并后以最新结构版本描述（字段语义等价）
	dst.SeenBy[src.SeqID] = true
	return mergedFields, ""
}

// AdvanceWatermark 显式推进水位（输入控制记录或墙钟），返回到点输出。
// 水位只许前进不许后退；后退返回错误由调用方记为拒绝。
func (p *Processor) AdvanceWatermark(wm time.Time) ([]emitted, error) {
	t := wm.UnixNano()
	if t < p.wm {
		return nil, fmt.Errorf("watermark regression %s -> %s", p.watermarkTime().Format(time.RFC3339Nano), wm.Format(time.RFC3339Nano))
	}
	p.wm = t
	return p.flushReady(), nil
}

// ForceFlushOldest 在缓冲压力下强制把水位推进到在途记录的最小事件时间，
// 输出最旧的一批记录。语义：突发流量导致缓冲达到上限时，为保证内存有界且不丢数据，
// 组件牺牲部分乱序等待（提前到点输出最旧记录），但绝不静默丢弃。
// 返回被输出的记录；无在途记录时返回 nil。
func (p *Processor) ForceFlushOldest() []emitted {
	if len(p.pending) == 0 {
		return nil
	}
	minET := p.pending[0].Ev.EventTime.UnixNano()
	for _, q := range p.pending[1:] {
		if et := q.Ev.EventTime.UnixNano(); et < minET {
			minET = et
		}
	}
	// 强制水位只进不退
	if minET > p.wm {
		p.wm = minET
	}
	return p.flushReady()
}

// FlushAll EOF 或优雅停机时，把水位推到最大值，输出全部剩余记录。
func (p *Processor) FlushAll() []emitted {
	if p.maxObs > p.wm {
		p.wm = p.maxObs
	}
	return p.flushReady()
}

// flushReady 输出 event_time <= watermark 的全部记录，按事件时间确定序排列。
func (p *Processor) flushReady() []emitted {
	if p.wm == 0 {
		return nil // 尚未建立任何水位：所有记录都必须等待，不能提前输出
	}
	var keep []*pendingRec
	var ready []*pendingRec
	for _, q := range p.pending {
		if q.Ev.EventTime.UnixNano() <= p.wm {
			ready = append(ready, q)
		} else {
			keep = append(keep, q)
		}
	}
	p.pending = keep
	if len(ready) == 0 {
		return nil
	}
	sort.SliceStable(ready, func(i, j int) bool {
		ti, tj := ready[i].Ev.EventTime.UnixNano(), ready[j].Ev.EventTime.UnixNano()
		if ti != tj {
			return ti < tj
		}
		if ready[i].Ev.Symbol != ready[j].Ev.Symbol {
			return ready[i].Ev.Symbol < ready[j].Ev.Symbol
		}
		return ready[i].Ev.SeqID < ready[j].Ev.SeqID
	})
	out := make([]emitted, 0, len(ready))
	for _, q := range ready {
		seq := p.allocSeq()
		// 已输出记录的去重条目保留到 watermark 越过其 event_time 之后再清理，
		// 因此这里仅清理 pending 归属；seen 清理在 sweepSeen 中进行。
		out = append(out, emitted{
			Ev:     q.Ev,
			Offset: q.Offset,
			Seq:    seq,
			Merged: len(q.SeenBy) > 1,
			Detail: mergeDetail(q),
		})
	}
	p.sweepSeen()
	return out
}

func mergeDetail(q *pendingRec) string {
	if len(q.SeenBy) <= 1 {
		return "on-time output by event_time"
	}
	ids := make([]string, 0, len(q.SeenBy))
	for id := range q.SeenBy {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return "coalesced from seq_ids=" + fmt.Sprint(ids)
}

// sweepSeen 清理去重条目（有界内存）。
// 记录在 watermark >= event_time+lateness 时到点输出；之后去重条目再保留一个
// 完整容忍窗口，期间重投仍判为“重复”；当 watermark 越过 event_time+2*lateness 后才回收。
func (p *Processor) sweepSeen() {
	retention := 2 * p.lateness.Nanoseconds()
	for id, s := range p.seen {
		if s.ET+retention < p.wm {
			delete(p.seen, id)
		}
	}
}
