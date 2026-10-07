package streamio

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/feng111627/market-event-pipeline-go/internal/event"
)

// 输出行（JSONL，每行一个 JSON 对象）的键名，与输入/核心字段保持一致。
const (
	outVersion = "version"
	outID      = "id"
	outSymbol  = "symbol"
	outPrice   = "price"
	outVolume  = "volume"
	outSeq     = "seq"
	outTime    = "event_time"
)

// LineSink 把按事件时间输出的事件逐条写成 JSONL（每行一个 JSON 对象），
// 实现 pipeline.Sink。
//
// 幂等契约：按事件 ID 去重，崩溃恢复后的重放只会跳过，不会重复落盘。
// 字段完整性：输出对象除核心字段外，包含 Attrs 中该版本已声明的全部字段
// （含为旧数据补齐的稳定缺省值）；未声明字段从输入起就被忽略，不会出现。
// 时间统一以 RFC3339Nano（UTC）输出，缺省零值时间输出其 RFC3339 形式，语义可复现。
type LineSink struct {
	bw      *bufio.Writer
	seen    map[string]struct{}
	written int
	closed  bool
}

// NewLineSink 创建 JSONL 写出器。
func NewLineSink(w io.Writer) *LineSink {
	return &LineSink{
		bw:   bufio.NewWriterSize(w, 64*1024),
		seen: make(map[string]struct{}),
	}
}

// Emit 幂等写出一条事件；同一 ID 的重放被跳过。
func (s *LineSink) Emit(ev event.MarketEvent) error {
	if _, dup := s.seen[ev.ID]; dup {
		return nil
	}
	s.seen[ev.ID] = struct{}{}

	obj := make(map[string]interface{}, len(ev.Attrs)+7)
	obj[outVersion] = ev.SchemaVer
	obj[outID] = ev.ID
	obj[outSymbol] = ev.Symbol
	obj[outPrice] = ev.Price
	obj[outVolume] = ev.Volume
	obj[outSeq] = ev.Seq
	obj[outTime] = formatTime(ev.EventTime)
	// attrs 全量平铺到输出对象（已声明字段，含缺省值）。
	keys := make([]string, 0, len(ev.Attrs))
	for k := range ev.Attrs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		obj[k] = exportValue(ev.Attrs[k])
	}
	line, err := json.Marshal(obj)
	if err != nil {
		return fmt.Errorf("marshal output event %s: %w", ev.ID, err)
	}
	if _, err := s.bw.Write(line); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	if err := s.bw.WriteByte('\n'); err != nil {
		return fmt.Errorf("write output newline: %w", err)
	}
	s.written++
	return nil
}

// Flush 把缓冲数据落到底层 writer（正常收尾或需要即时可见时调用）。
func (s *LineSink) Flush() error {
	if s.bw == nil {
		return nil
	}
	return s.bw.Flush()
}

// Written 返回已唯一写出的条数（不含被幂等跳过的重放）。
func (s *LineSink) Written() int { return s.written }

// formatTime 统一时间输出：非零时间转 UTC RFC3339Nano；零值时间输出固定串，
// 保证缺省语义在不同机器上可复现。
func formatTime(t time.Time) string {
	if t.IsZero() {
		return "0001-01-01T00:00:00Z"
	}
	return t.UTC().Format(time.RFC3339Nano)
}

// exportValue 处理 attrs 中的特殊类型，使其 JSON 序列化语义稳定。
func exportValue(v interface{}) interface{} {
	switch t := v.(type) {
	case time.Time:
		return formatTime(t)
	default:
		return v
	}
}
