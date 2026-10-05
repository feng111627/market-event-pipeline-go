// Package ingress 负责从本地输入逐行读取原始记录并跟踪字节偏移。
package ingress

import (
	"bufio"
	"errors"
	"io"

	"github.com/feng111627/market-event-pipeline-go/internal/event"
)

// ErrEOF 输入结束（即 io.EOF，便于调用方区分来源）。
var ErrEOF = io.EOF

// Reader 逐行产出带字节偏移的原始输入。
// Offset 始终等于“已从底层读取消费的字节总数”，即下一行的起始字节位置，
// 该值被断点用于精确定位恢复点（包含换行符本身）。
type Reader struct {
	r      *bufio.Reader
	offset int64
}

// NewReader 包装一个输入流。
func NewReader(r io.Reader) *Reader {
	return &Reader{r: bufio.NewReaderSize(r, 64*1024)}
}

// SeekReader 是可定位输入的能力接口（*os.File 天然满足）。
type SeekReader interface {
	io.Reader
	Seek(offset int64, whence int) (int64, error)
}

// NewReaderAt 从指定字节偏移开始读取（断点恢复用）。
func NewReaderAt(r SeekReader, offset int64) (*Reader, error) {
	if _, err := r.Seek(offset, io.SeekStart); err != nil {
		return nil, err
	}
	return &Reader{r: bufio.NewReaderSize(r, 64*1024), offset: offset}, nil
}

// Next 读取下一行。
// 返回的 Source.Offset 为该行起始字节偏移；读取后 Reader.Offset() 推进到下一行起点。
// 空行被跳过（偏移照常推进）；流结束返回 io.EOF。
func (r *Reader) Next() (event.Source, error) {
	for {
		start := r.offset
		line, err := r.r.ReadBytes('\n')
		r.offset += int64(len(line))
		content := trimEOL(line)
		if len(content) > 0 {
			return event.Source{Offset: start, Line: string(content)}, nil
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return event.Source{}, ErrEOF
			}
			return event.Source{}, err
		}
		// 读到的是空行：继续读下一行，start 已随 offset 跳过空行字节
	}
}

// Offset 返回下一行的起始字节偏移（= 已消费字节数）。
func (r *Reader) Offset() int64 { return r.offset }

func trimEOL(b []byte) []byte {
	if len(b) > 0 && b[len(b)-1] == '\n' {
		b = b[:len(b)-1]
	}
	if len(b) > 0 && b[len(b)-1] == '\r' {
		b = b[:len(b)-1]
	}
	return b
}
