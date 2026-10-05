package ingress

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReader_OffsetsAreByteExact(t *testing.T) {
	t.Log("【业务覆盖】输入偏移：每行 Offset 为行首字节位置，Reader.Offset 为行末（含换行），供断点精确定位")
	input := "abc\n\nhello\n" // 含一个空行，应被跳过但字节要记账
	r := NewReader(strings.NewReader(input))

	s1, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if s1.Line != "abc" || s1.Offset != 0 || r.Offset() != 4 {
		t.Fatalf("line1 got offset=%d end=%d line=%q", s1.Offset, r.Offset(), s1.Line)
	}
	t.Logf("  ✔ 第一行 offset=%d，行末 offset=%d（空行被跳过但字节已计）", s1.Offset, r.Offset())

	s2, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if s2.Line != "hello" || s2.Offset != 5 || r.Offset() != 11 {
		t.Fatalf("line2 got offset=%d end=%d line=%q", s2.Offset, r.Offset(), s2.Line)
	}
	t.Logf("  ✔ 第二行 offset=%d（5=4+空行1字节），行末 offset=%d=文件总长", s2.Offset, r.Offset())

	if _, err := r.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("want io.EOF, got %v", err)
	}
	if r.Offset() != int64(len(input)) {
		t.Fatalf("final offset %d != file len %d", r.Offset(), len(input))
	}
	t.Log("  ✔ EOF 后偏移精确等于输入字节数，恢复定位不会重读/漏读半行")
}

func TestReaderAt_ResumeFromOffset(t *testing.T) {
	t.Log("【业务覆盖】断点恢复定位：NewReaderAt 从指定字节偏移继续，得到与连续读取一致的剩余行")
	input := "row0\nrow1\nrow2\n"
	path := filepath.Join(t.TempDir(), "in.txt")
	if err := os.WriteFile(path, []byte(input), 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// "row0\n" 占 5 字节，从 5 恢复应直接得到 row1
	r, err := NewReaderAt(f, 5)
	if err != nil {
		t.Fatal(err)
	}
	s, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if s.Line != "row1" || s.Offset != 5 {
		t.Fatalf("resume got line=%q offset=%d, want row1 @5", s.Line, s.Offset)
	}
	t.Log("  ✔ 从断点偏移 5 恢复，下一条恰好是 row1（不多不少）")
}
