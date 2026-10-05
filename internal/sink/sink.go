// Package sink 提供按事件时间排序的结果输出，并保证恢复时不重复计入。
//
// 恰好一次协作：
//   - 每条结论带单调递增的 Seq；FileSink 只接受 Seq==ack+1 的写入，
//     重放产生的旧 Seq 会被拒绝（ErrAlreadyAcked），不会重复计入；
//   - 启动时以 sidecar 记录的已确认序号为准，截断输出文件中“已写未确认”的尾部
//     （模拟崩溃可能留下的半条/多余行），使输出位置回到提交点。
package sink

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/feng111627/market-event-pipeline-go/internal/event"
)

// Outcome 落向 sink 的一条最终结论记录。
type Outcome struct {
	Seq       int64            `json:"seq"`
	Reason    event.Reason     `json:"reason"`
	Detail    string           `json:"detail"`
	EventTime time.Time        `json:"event_time"`
	Symbol    string           `json:"symbol"`
	SeqID     string           `json:"seq_id"`
	Price     int64            `json:"price"`
	Qty       int64            `json:"qty"`
	SchemaV   int              `json:"schema_v"`
	Extra     map[string]int64 `json:"extra,omitempty"`
}

// Sink 结果输出端。
type Sink interface {
	// Emit 输出一条结论。Seq 必须严格按 ack+1 递增，否则返回 ErrAlreadyAcked。
	Emit(ctx context.Context, o Outcome) error
	// AcknowledgedSeq 返回已持久化确认的最大序号。
	AcknowledgedSeq() int64
	Close() error
}

// ErrAlreadyAcked 该序号（或更早）已确认，重放记录不得重复计入。
var ErrAlreadyAcked = errors.New("outcome already acknowledged")

// ErrOutOfOrderSeq 输出序号出现空洞/回退（编程错误，必须显式暴露）。
var ErrOutOfOrderSeq = errors.New("outcome seq out of order")

// FileSink 把结果以 JSONL 追加到文件，sidecar 记录已确认序号；每次 Emit 即刷盘。
type FileSink struct {
	mu     sync.Mutex
	path   string
	side   string
	f      *os.File
	w      *bufio.Writer
	ack    int64
	closed bool
}

// OpenFileSink 打开（或恢复）文件 sink。输出文件会被截断到 sidecar 记录的确认位置。
func OpenFileSink(path string) (*FileSink, error) {
	side := path + ".ack"
	if err := truncateToAck(path, side); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	ack, err := readAck(side)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return &FileSink{
		path: path, side: side, f: f,
		w: bufio.NewWriterSize(f, 64*1024), ack: ack,
	}, nil
}

// NewFileSink 从零创建 sink（便捷构造，供 demo 使用）。
func NewFileSink(path string) *FileSink {
	s, err := OpenFileSink(path)
	if err != nil {
		panic(err)
	}
	return s
}

// Emit 追加一条结论并立即刷盘、更新 sidecar（确认点随输出同步推进）。
func (s *FileSink) Emit(ctx context.Context, o Outcome) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if o.Seq <= s.ack {
		return fmt.Errorf("%w: seq=%d ack=%d", ErrAlreadyAcked, o.Seq, s.ack)
	}
	if o.Seq != s.ack+1 {
		return fmt.Errorf("%w: seq=%d expected=%d", ErrOutOfOrderSeq, o.Seq, s.ack+1)
	}
	b, err := json.Marshal(o)
	if err != nil {
		return err
	}
	if _, err := s.w.Write(b); err != nil {
		return err
	}
	if err := s.w.WriteByte('\n'); err != nil {
		return err
	}
	if err := s.w.Flush(); err != nil {
		return err
	}
	if err := s.f.Sync(); err != nil {
		return err
	}
	if err := os.WriteFile(s.side, []byte(strconv.FormatInt(o.Seq, 10)), 0o644); err != nil {
		return err
	}
	s.ack = o.Seq
	return nil
}

// AcknowledgedSeq 已确认的最大序号。
func (s *FileSink) AcknowledgedSeq() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ack
}

// Close 刷盘并关闭。
func (s *FileSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	if err := s.w.Flush(); err != nil {
		_ = s.f.Close()
		return err
	}
	return s.f.Close()
}

func readAck(side string) (int64, error) {
	b, err := os.ReadFile(side)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
}

// truncateToAck 统计输出文件中完整 JSONL 行数，丢弃超过 ack 的尾部（崩溃残留）。
func truncateToAck(path, side string) error {
	ack, err := readAck(side)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 16*1024*1024)
	var lines int64
	var validBytes int64
	for sc.Scan() {
		line := sc.Bytes()
		if len(strings.TrimSpace(string(line))) == 0 {
			continue
		}
		if lines < ack {
			// 已确认区域必须是合法 JSON，否则文件已损坏，拒绝从中间瞎接
			var probe map[string]any
			if err := json.Unmarshal(line, &probe); err != nil {
				return fmt.Errorf("output corrupt within acknowledged region: %w", err)
			}
			validBytes += int64(len(line)) + 1
		}
		lines++
	}
	if err := sc.Err(); err != nil {
		return err
	}
	if lines < ack {
		return fmt.Errorf("output has %d lines but ack=%d: %w", lines, ack, io.ErrUnexpectedEOF)
	}
	if lines > ack {
		if err := f.Truncate(validBytes); err != nil {
			return err
		}
	}
	return nil
}

// MemorySink 测试用内存 sink，按同样的 Seq 幂等规则工作。
type MemorySink struct {
	mu    sync.Mutex
	items []Outcome
	ack   int64
}

// Emit 内存输出。
func (m *MemorySink) Emit(_ context.Context, o Outcome) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if o.Seq <= m.ack {
		return fmt.Errorf("%w: seq=%d ack=%d", ErrAlreadyAcked, o.Seq, m.ack)
	}
	if o.Seq != m.ack+1 {
		return fmt.Errorf("%w: seq=%d expected=%d", ErrOutOfOrderSeq, o.Seq, m.ack+1)
	}
	m.items = append(m.items, o)
	m.ack = o.Seq
	return nil
}

// AcknowledgedSeq 已确认序号。
func (m *MemorySink) AcknowledgedSeq() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ack
}

// Close 空实现。
func (m *MemorySink) Close() error { return nil }

// Items 返回已确认输出的副本。
func (m *MemorySink) Items() []Outcome {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Outcome, len(m.items))
	copy(out, m.items)
	return out
}
