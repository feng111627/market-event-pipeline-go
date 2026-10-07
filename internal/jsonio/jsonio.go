// Package jsonio 提供行情事件流水线的按行 JSON 输入输出。
//
// 输入每行一条 JSON 记录，并标明自己的结构版本，支持两种写法：
//
//	{"version":2,"data":{"id":"t1","symbol":"BTCUSDT",...}}
//	{"version":2,"id":"t1","symbol":"BTCUSDT",...}
//
// 缺省 version 时按 1 处理。无法解析的行不会中断整批处理，而是以
// ParseError 记录上抛，由流水线计为 REJECTED 并在汇总中可见。
//
// 输出每行一条扁平 JSON：核心字段在前，该事件结构版本声明的全部扩展
// 字段（含按缺省值补齐的字段）紧随其后，并带 "schema_version"；
// 记录中未在结构里声明的字段不会出现。
package jsonio

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/feng111627/market-event-pipeline-go/internal/event"
	"github.com/feng111627/market-event-pipeline-go/internal/pipeline"
)

// StdinPath / StdoutPath 是标准输入/输出的保留路径。
const (
	StdinPath  = "-"
	StdoutPath = "-"
)

// defaultVersion 是记录未声明版本时采用的结构版本。
const defaultVersion = 1

// envelope 对应 {"version":N,"data":{...}} 的包装写法。
type envelope struct {
	Version *int                   `json:"version"`
	Data    map[string]interface{} `json:"data"`
}

// LineReader 按行读取记录，并维护字节位点（用于断点恢复）。
type LineReader struct {
	r      *bufio.Reader
	offset int64 // 下一行起始字节位点
	line   int   // 已返回的行数
}

// NewLineReader 从 r 创建按行读取器；startOffset 为起始字节位点（0 起）。
func NewLineReader(r io.Reader, startOffset int64) *LineReader {
	return &LineReader{r: bufio.NewReaderSize(r, 1<<20), offset: startOffset}
}

// ReadLine 返回下一条源记录；ok=false 表示输入耗尽。
func (lr *LineReader) ReadLine() (pipeline.SourceRecord, bool) {
	raw, err := lr.r.ReadBytes('\n')
	n := int64(len(raw))
	start := lr.offset
	lr.offset += n
	if len(raw) == 0 {
		return pipeline.SourceRecord{}, false
	}
	lr.line++
	line := strings.TrimRight(string(raw), "\r\n")
	rec := pipeline.SourceRecord{Offset: start, Line: lr.line}
	if err != nil && err != io.EOF {
		rec.ParseError = fmt.Sprintf("read error: %v", err)
		return rec, true
	}
	if strings.TrimSpace(line) == "" {
		if err == io.EOF {
			return pipeline.SourceRecord{}, false
		}
		rec.ParseError = "empty line"
		return rec, true
	}
	parseLine(line, &rec)
	return rec, true
}

// parseLine 解析单行 JSON，支持包装写法与扁平写法。
func parseLine(line string, rec *pipeline.SourceRecord) {
	var env envelope
	if err := json.Unmarshal([]byte(line), &env); err != nil {
		rec.ParseError = fmt.Sprintf("invalid JSON: %v", err)
		return
	}
	if env.Data != nil {
		rec.SchemaVersion = versionOf(env.Version)
		rec.Payload = env.Data
		return
	}
	// 扁平写法：整体是一条记录。
	var flat map[string]interface{}
	if err := json.Unmarshal([]byte(line), &flat); err != nil {
		rec.ParseError = fmt.Sprintf("invalid JSON: %v", err)
		return
	}
	rec.SchemaVersion = versionFromMap(flat)
	rec.Payload = flat
}

func versionOf(v *int) int {
	if v == nil {
		return defaultVersion
	}
	return *v
}

// versionFromMap 从扁平记录里取顶层 version 字段；缺失或非法则用默认版本。
func versionFromMap(m map[string]interface{}) int {
	raw, ok := m["version"]
	if !ok {
		return defaultVersion
	}
	switch v := raw.(type) {
	case float64:
		return int(v)
	case int:
		return v
	default:
		return defaultVersion
	}
}

// FileSource 从文件或标准输入按行读取，实现 pipeline.Source。
type FileSource struct {
	path  string
	f     *os.File
	stdin bool
	lr    *LineReader
}

// OpenFileSource 打开输入源；path 为 "-" 时使用标准输入（不可断点恢复）。
func OpenFileSource(path string) (*FileSource, error) {
	if path == StdinPath {
		return &FileSource{path: path, f: os.Stdin, stdin: true, lr: NewLineReader(os.Stdin, 0)}, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open input %s: %w", path, err)
	}
	return &FileSource{path: path, f: f, lr: NewLineReader(f, 0)}, nil
}

// Next 返回下一条记录。
func (s *FileSource) Next() (pipeline.SourceRecord, bool) {
	return s.lr.ReadLine()
}

// SeekTo 定位字节位点；标准输入不支持。
func (s *FileSource) SeekTo(offset int64) error {
	if s.stdin {
		if offset != 0 {
			return fmt.Errorf("cannot seek stdin to offset %d: standard input is not restartable", offset)
		}
		return nil
	}
	if _, err := s.f.Seek(offset, io.SeekStart); err != nil {
		return fmt.Errorf("seek input: %w", err)
	}
	s.lr = NewLineReader(s.f, offset)
	return nil
}

// Close 关闭底层文件（标准输入不关闭）。
func (s *FileSource) Close() error {
	if s.stdin {
		return nil
	}
	return s.f.Close()
}

// OutputRecord 是一行输出的扁平表示：核心字段 + 全部已声明扩展字段。
type OutputRecord struct {
	SchemaVersion int                    `json:"schema_version"`
	ID            string                 `json:"id"`
	Symbol        string                 `json:"symbol"`
	Price         float64                `json:"price"`
	Volume        float64                `json:"volume"`
	Seq           uint64                 `json:"seq"`
	EventTime     string                 `json:"event_time"`
	Attrs         map[string]interface{} `json:"-"`
}

// MarshalJSON 输出扁平对象：核心字段固定在前，扩展字段按名排序追加在后。
func (o OutputRecord) MarshalJSON() ([]byte, error) {
	var b strings.Builder
	b.WriteString(`{"schema_version":`)
	writeJSON(&b, float64(o.SchemaVersion))
	writeCore(&b, "id", o.ID)
	writeCore(&b, "symbol", o.Symbol)
	writeCore(&b, "price", o.Price)
	writeCore(&b, "volume", o.Volume)
	writeCore(&b, "seq", o.Seq)
	writeCore(&b, "event_time", o.EventTime)
	keys := make([]string, 0, len(o.Attrs))
	for k := range o.Attrs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		b.WriteByte(',')
		writeJSONString(&b, k)
		b.WriteByte(':')
		writeJSON(&b, attrForJSON(o.Attrs[k]))
	}
	b.WriteByte('}')
	return []byte(b.String()), nil
}

func writeCore(b *strings.Builder, key string, v interface{}) {
	b.WriteByte(',')
	writeJSONString(b, key)
	b.WriteByte(':')
	writeJSON(b, v)
}

// attrForJSON 把 time.Time 类型的扩展字段格式化为 RFC3339Nano，
// 其余类型交给 encoding/json 的标量编码。
func attrForJSON(v interface{}) interface{} {
	if ts, ok := v.(time.Time); ok {
		return ts.UTC().Format(time.RFC3339Nano)
	}
	return v
}

func writeJSON(b *strings.Builder, v interface{}) {
	data, err := json.Marshal(v)
	if err != nil {
		data, _ = json.Marshal(nil)
	}
	b.Write(data)
}

func writeJSONString(b *strings.Builder, s string) {
	data, _ := json.Marshal(s)
	b.Write(data)
}

// ToOutputRecord 把内部事件转为扁平输出记录。
func ToOutputRecord(schemaVersion int, ev event.MarketEvent) OutputRecord {
	return OutputRecord{
		SchemaVersion: schemaVersion,
		ID:            ev.ID,
		Symbol:        ev.Symbol,
		Price:         ev.Price,
		Volume:        ev.Volume,
		Seq:           ev.Seq,
		EventTime:     ev.EventTime.UTC().Format(time.RFC3339Nano),
		Attrs:         ev.Attrs,
	}
}

// FileSink 把输出按行写入文件或标准输出，实现 pipeline.DurableSink。
//
// 写入带缓冲；Sync 强制落盘并推进持久化字节位点，供断点协同。
// 恢复时流水线调用 Truncate：无断点截到 0（全新开始），有断点截到
// 断点记录的 sink 位点（回滚未落盘部分）。因此打开文件时不主动 O_TRUNC，
// 以免在恢复前清掉需要保留的前缀。
type FileSink struct {
	f         *os.File
	stdout    bool
	w         *bufio.Writer
	committed int64  // 已 Sync 的持久化字节位点
	dirty     int64  // 缓冲区中未落盘字节数
	onEmit    func() // 测试钩子：每次成功写入一行后回调
}

// SetEmitHook 注册每次 Emit 成功后的回调（仅测试用，如模拟中途崩溃）。
func (s *FileSink) SetEmitHook(h func()) { s.onEmit = h }

// OpenFileSink 打开输出；path 为 "-" 时写标准输出（Sync 仅 flush，
// 不支持 Truncate，因此标准输出场景不做断点恢复）。
func OpenFileSink(path string) (*FileSink, error) {
	if path == StdoutPath {
		return &FileSink{f: os.Stdout, stdout: true, w: bufio.NewWriterSize(os.Stdout, 1<<20)}, nil
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open output %s: %w", path, err)
	}
	return &FileSink{f: f, w: bufio.NewWriterSize(f, 1<<20)}, nil
}

// Emit 写出一行事件（按事件时间有序，由流水线保证）。
func (s *FileSink) Emit(ev event.MarketEvent) error {
	rec := ToOutputRecord(ev.SchemaVersion(), ev)
	data, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("marshal output: %w", err)
	}
	n, err := s.w.Write(append(data, '\n'))
	if err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	s.dirty += int64(n)
	if s.onEmit != nil {
		s.onEmit()
	}
	return nil
}

// Sync 刷新缓冲并强制文件内容落盘，再推进持久化字节位点。
func (s *FileSink) Sync() error {
	if err := s.w.Flush(); err != nil {
		return fmt.Errorf("flush output: %w", err)
	}
	if !s.stdout {
		if err := s.f.Sync(); err != nil {
			return fmt.Errorf("fsync output: %w", err)
		}
	}
	s.committed += s.dirty
	s.dirty = 0
	return nil
}

// DurableOffset 返回已持久化输出对应的字节位点。
func (s *FileSink) DurableOffset() int64 { return s.committed }

// Truncate 丢弃缓冲并把物理文件截断到 offset，写位点随之回到 offset。
func (s *FileSink) Truncate(offset int64) error {
	if s.stdout {
		if offset == 0 {
			return nil
		}
		return fmt.Errorf("cannot truncate stdout: use file output for restartable runs")
	}
	s.w.Reset(s.f)
	s.dirty = 0
	if err := s.f.Truncate(offset); err != nil {
		return fmt.Errorf("truncate output: %w", err)
	}
	if _, err := s.f.Seek(offset, io.SeekStart); err != nil {
		return fmt.Errorf("seek output after truncate: %w", err)
	}
	s.committed = offset
	return nil
}

// Close 刷新并关闭（标准输出只 flush 不关闭）。
func (s *FileSink) Close() error {
	if err := s.w.Flush(); err != nil {
		return err
	}
	if s.stdout {
		return nil
	}
	return s.f.Close()
}
