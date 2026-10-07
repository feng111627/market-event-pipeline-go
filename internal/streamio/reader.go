// Package streamio 提供按行流式读取（文件或标准输入）与按行 JSONL 写出
// （文件或标准输出）的外部数据出入口。
//
// 读取按行进行、逐条投递到流水线，读取端不囤积整个输入文件；
// 写出同样逐条进行，因此内存占用与输入文件大小解耦（可处理大于内存的文件）。
package streamio

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/feng111627/market-event-pipeline-go/internal/pipeline"
)

// 输入行的信封格式（每行一个 JSON 对象）：
//
//	{"version": 2, "payload": { ...该结构版本的字段... }}
//
// version 标明本行使用的结构版本；payload 为该版本字段。
// 为便于手工造数，也接受顶层平铺形式：除保留键 "version" 外的其余顶层
// 字段整体作为 payload（与信封形式等价，不允许重复的嵌套 payload）。
const (
	keyVersion = "version"
	keyPayload = "payload"
)

// maxBadLinePreview 限制单条坏行在内存与汇总中保留的预览字节数。
const maxBadLinePreview = 256

// maxLineLen 是单行输入的业务硬上界（字节）。超过的行整条记为坏行并跳过，
// 其字节仍被读出丢弃以保证位点连续，从而单行异常不会撑爆内存。
const maxLineLen = 8 * 1024 * 1024

// BadLine 描述一条无法解析的输入行。
type BadLine struct {
	Line   int    // 行号（从 1 开始）
	Reason string // 解析失败原因
	Raw    string // 原始行内容（可能很长，仅用于诊断；汇总中不强制展示）
}

// LineSource 按行读取 JSON 记录的数据源，实现 pipeline.Source。
//
// 位点语义：Offset 为"下一条待读行首的字节偏移"，与断点提交的
// rec.Offset+1 一致，因此断点恢复时可直接 SeekTo 到字节位置继续读，
// 已提交的行不会重读，未提交的行由下游幂等吸收，最终不多不少。
type LineSource struct {
	br         *bufio.Reader
	underlying io.Reader
	pos        int64 // 当前读指针的字节偏移
	line       int   // 当前行号（从 1 开始）
	seekable   bool  // 底层是否支持定位（文件支持；管道/标准输入不支持）
	seeker     io.Seeker
	bad        []BadLine
}

// NewLineSource 从 r 创建按行数据源。若 r 同时实现 io.Seeker（如 *os.File），
// 则支持按字节位点断点恢复；否则只能从头顺序读取。
func NewLineSource(r io.Reader) *LineSource {
	s := &LineSource{br: bufio.NewReaderSize(r, 64*1024), underlying: r}
	if sk, ok := r.(io.Seeker); ok {
		s.seeker = sk
		s.seekable = true
	}
	return s
}

// Seekable 报告底层输入是否支持按位点恢复。
func (s *LineSource) Seekable() bool { return s.seekable }

// SeekTo 将读取位置定位到指定字节偏移。标准输入等不可定位源在 offset>0 时
// 明确报错，避免从错误的位置接着算。
func (s *LineSource) SeekTo(offset int64) error {
	if offset < 0 {
		return fmt.Errorf("seek offset %d out of range", offset)
	}
	if !s.seekable {
		if offset == 0 {
			return nil
		}
		return fmt.Errorf("input is not seekable; cannot resume at byte offset %d (use a file input or start over)", offset)
	}
	if _, err := s.seeker.Seek(offset, io.SeekStart); err != nil {
		return fmt.Errorf("seek to %d: %w", offset, err)
	}
	s.br.Reset(s.brForReset())
	s.pos = offset
	s.line = 0
	return nil
}

// brForReset 在 Seek 后让 bufio.Reader 重新绑定底层 reader。
// 见 NewLineSource 中保存的原始 reader。
func (s *LineSource) brForReset() io.Reader { return s.underlying }

// Next 读取并解析下一行；空行跳过；解析失败或超长的行记为坏行并继续读取
// 后续行（坏行不进入流水线，另由 BadLines 暴露给汇总）。
func (s *LineSource) Next() (pipeline.SourceRecord, bool) {
	for {
		raw, n, oversize, err := s.readBoundedLine()
		if n > 0 {
			lineStart := s.pos
			s.pos += int64(n)
			s.line++
			lineNo := s.line
			if oversize {
				s.bad = append(s.bad, BadLine{
					Line:   lineNo,
					Reason: fmt.Sprintf("line exceeds max length %d bytes", maxLineLen),
					Raw:    string(raw),
				})
				if err == io.EOF {
					return pipeline.SourceRecord{}, false
				}
				continue
			}
			trimmed := bytes.TrimRight(raw, "\r\n")
			if len(bytes.TrimSpace(trimmed)) == 0 {
				// 空行不计入数据，继续读下一行（位点仍已正确推进）。
				if err == io.EOF {
					return pipeline.SourceRecord{}, false
				}
				continue
			}
			rec, perr := parseLine(trimmed)
			if perr != nil {
				// 坏行只保留截断预览，避免一整行超大 JSON 常驻内存。
				preview := trimmed
				if len(preview) > maxBadLinePreview {
					preview = preview[:maxBadLinePreview]
				}
				s.bad = append(s.bad, BadLine{Line: lineNo, Reason: perr.Error(), Raw: string(preview)})
				if err == io.EOF {
					return pipeline.SourceRecord{}, false
				}
				continue
			}
			rec.Offset = lineStart
			rec.NextOffset = lineStart + int64(n) // 含行尾，即下一行首字节偏移
			return rec, true
		}
		if err != nil {
			return pipeline.SourceRecord{}, false
		}
	}
}

// readBoundedLine 读取至多一行，并对行长设硬上界：
//   - 正常行：data 为整行内容，consumed 为该行实际字节数（含行尾），oversize=false。
//   - 超长行（超过 maxLineLen）：data 仅为前 maxBadLinePreview 字节预览；
//     consumed 仍为整行字节数（超长部分被读出丢弃），oversize=true。
//
// err=io.EOF 表示读到输入结束。
func (s *LineSource) readBoundedLine() (data []byte, consumed int, oversize bool, err error) {
	var parts [][]byte // 分块读取时暂存（行长 <= maxLineLen 时保留全量；超长后置空）
	total := 0
	tooLong := false
	for {
		chunk, rerr := s.br.ReadBytes('\n')
		consumed += len(chunk)
		if rerr == bufio.ErrBufferFull {
			// 一行比内部读缓冲还长，尚未见到行尾：分块处理。
			if !tooLong {
				if total+len(chunk) > maxLineLen {
					tooLong = true
					parts = nil // 确认超长，释放已累积内容，之后只计数丢弃
				} else {
					total += len(chunk)
					parts = append(parts, append([]byte(nil), chunk...))
				}
			}
			continue
		}
		if rerr != nil && rerr != io.EOF {
			return nil, consumed, false, rerr
		}
		// 到达行尾或文件尾。
		if tooLong || total+len(chunk) > maxLineLen {
			return s.previewFromParts(parts, chunk), consumed, true, rerr
		}
		if len(parts) > 0 {
			return bytes.Join(append(parts, chunk), nil), consumed, false, rerr
		}
		if len(chunk) > maxLineLen {
			return append([]byte(nil), chunk[:maxBadLinePreview]...), consumed, true, rerr
		}
		return chunk, consumed, false, rerr
	}
}

// previewFromParts 从已保留分块与结尾块中拼出至多 maxBadLinePreview 字节预览。
func (s *LineSource) previewFromParts(parts [][]byte, tail []byte) []byte {
	preview := make([]byte, 0, maxBadLinePreview)
	for _, p := range parts {
		room := maxBadLinePreview - len(preview)
		if room <= 0 {
			return preview
		}
		if room > len(p) {
			room = len(p)
		}
		preview = append(preview, p[:room]...)
	}
	room := maxBadLinePreview - len(preview)
	if room > 0 {
		if room > len(tail) {
			room = len(tail)
		}
		preview = append(preview, tail[:room]...)
	}
	return preview
}

// parseLine 解析一行为 SourceRecord，支持信封与平铺两种形式。
func parseLine(raw []byte) (pipeline.SourceRecord, error) {
	var top map[string]json.RawMessage
	if err := json.Unmarshal(raw, &top); err != nil {
		return pipeline.SourceRecord{}, fmt.Errorf("invalid json: %w", err)
	}
	vraw, hasVersion := top[keyVersion]
	if !hasVersion {
		return pipeline.SourceRecord{}, fmt.Errorf("missing %q", keyVersion)
	}
	var version int
	if err := json.Unmarshal(vraw, &version); err != nil {
		return pipeline.SourceRecord{}, fmt.Errorf("decode %q: %w", keyVersion, err)
	}
	payloadRaw, hasPayload := top[keyPayload]
	var payload map[string]interface{}
	if hasPayload {
		if err := json.Unmarshal(payloadRaw, &payload); err != nil {
			return pipeline.SourceRecord{}, fmt.Errorf("decode %q: %w", keyPayload, err)
		}
	} else {
		// 平铺形式：除保留键外其余顶层字段作为 payload。
		payload = make(map[string]interface{}, len(top))
		for k, v := range top {
			if k == keyVersion {
				continue
			}
			var val interface{}
			if err := json.Unmarshal(v, &val); err != nil {
				return pipeline.SourceRecord{}, fmt.Errorf("decode field %q: %w", k, err)
			}
			payload[k] = val
		}
	}
	return pipeline.SourceRecord{SchemaVersion: version, Payload: payload}, nil
}

// BadLines 返回读取过程中遇到的无法解析的行（按出现顺序）。
func (s *LineSource) BadLines() []BadLine {
	out := make([]BadLine, len(s.bad))
	copy(out, s.bad)
	return out
}
