package streamio

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/feng111627/market-event-pipeline-go/internal/event"
)

// TestLineSourceEnvelopeAndFlat 覆盖信封/平铺两种输入形式、空行跳过与坏行记录。
func TestLineSourceEnvelopeAndFlat(t *testing.T) {
	t.Log("业务场景: 外部按行喂数，支持信封与平铺形式，空行跳过，坏行单独上报不污染流水线")
	input := strings.Join([]string{
		`{"version":2,"payload":{"id":"e1","symbol":"S","price":1.5,"volume":2,"seq":1,"event_time":"2026-10-06T10:00:01Z"}}`,
		``,
		`{"version":3,"id":"e2","symbol":"S","price":2.5,"volume":3,"seq":1,"event_time":"2026-10-06T10:00:02Z"}`, // 平铺
		`not-json`,
		`{"id":"e3"}`, // 缺 version
		"",
	}, "\n")
	src := NewLineSource(strings.NewReader(input))
	var got []struct {
		ver int
		id  string
	}
	for {
		rec, ok := src.Next()
		if !ok {
			break
		}
		got = append(got, struct {
			ver int
			id  string
		}{rec.SchemaVersion, rec.Payload["id"].(string)})
	}
	if len(got) != 2 || got[0].id != "e1" || got[0].ver != 2 || got[1].id != "e2" || got[1].ver != 3 {
		t.Fatalf("解析记录不符合预期: %+v", got)
	}
	bad := src.BadLines()
	if len(bad) != 2 {
		t.Fatalf("坏行数 = %d, 期望 2: %+v", len(bad), bad)
	}
	t.Logf("用例结果: 信封/平铺各 1 条正常，空行跳过，坏行 %d 条（%s）[通过]", len(bad), bad[0].Reason)
}

// TestLineSourceSeekToFile 覆盖按字节位点的断点定位（文件可定位）。
func TestLineSourceSeekToFile(t *testing.T) {
	t.Log("业务场景: 断点恢复时按字节位点重新定位文件，已提交行不重读")
	path := filepath.Join(t.TempDir(), "in.jsonl")
	lines := []string{
		`{"version":1,"payload":{"id":"l1"}}` + "\n",
		`{"version":1,"payload":{"id":"l2"}}` + "\n",
		`{"version":1,"payload":{"id":"l3"}}` + "\n",
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "")), 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	src := NewLineSource(f)

	// 读前两行，记录第二行的 NextOffset（第三行首字节偏移）。
	if _, ok := src.Next(); !ok {
		t.Fatal("应读到 l1")
	}
	r2, ok := src.Next()
	if !ok {
		t.Fatal("应读到 l2")
	}
	if r2.Payload["id"] != "l2" {
		t.Fatalf("第二行 = %v", r2.Payload["id"])
	}
	// 定位到第三行起始。
	if err := src.SeekTo(r2.NextOffset); err != nil {
		t.Fatalf("SeekTo: %v", err)
	}
	r3, ok := src.Next()
	if !ok || r3.Payload["id"] != "l3" {
		t.Fatalf("恢复后应读到 l3, 得到 %v ok=%v", r3.Payload["id"], ok)
	}
	if _, ok := src.Next(); ok {
		t.Fatal("读到 l3 后应耗尽")
	}
	t.Logf("用例结果: 从字节 %d 恢复后只读到 l3，前两行未重读 [通过]", r2.NextOffset)
}

// TestLineSourceSeekNonSeekable 验证标准输入（不可定位）在非零位点恢复时明确报错。
func TestLineSourceSeekNonSeekable(t *testing.T) {
	t.Log("业务场景: 管道/标准输入不支持按位点恢复，非零 Seek 必须明确拒绝而非错算")
	src2 := NewLineSource(onlyReader{data: []byte("abc")})
	if err := src2.SeekTo(0); err != nil {
		t.Fatalf("offset=0 应允许: %v", err)
	}
	if src2.Seekable() {
		t.Fatal("onlyReader 不应被识别为可定位")
	}
	if err := src2.SeekTo(3); err == nil {
		t.Fatal("不可定位输入 seek 到 3 应报错")
	}
	t.Log("用例结果: 非可定位输入仅允许从头读，非零位点被拒绝 [通过]")
}

type onlyReader struct{ data []byte }

func (o onlyReader) Read(p []byte) (int, error) {
	n := copy(p, o.data)
	if n == 0 {
		return 0, io.EOF
	}
	o.data = o.data[n:]
	return n, nil
}

// TestLineSinkEmitsAllDeclaredFields 覆盖核心字段 + 全部 attrs（含缺省）完整输出，
// 以及按 ID 幂等（重放不重复落盘）。
func TestLineSinkEmitsAllDeclaredFields(t *testing.T) {
	t.Log("业务场景: 新增可选字段（含缺省值）必须完整出现在最终输出，重放不重复")
	var buf bytes.Buffer
	sink := NewLineSink(&buf)
	ev := event.MarketEvent{
		ID: "e1", Symbol: "S", Price: 1.5, Volume: 2, Seq: 1,
		EventTime: time.Date(2026, 10, 6, 10, 0, 1, 0, time.UTC),
		SchemaVer: 3,
		Attrs: map[string]interface{}{
			"exchange":  "COINBASE",
			"venue_mic": "COIN",
			"is_maker":  true,
			"at":        time.Date(2026, 10, 6, 10, 0, 1, 0, time.UTC),
		},
	}
	if err := sink.Emit(ev); err != nil {
		t.Fatal(err)
	}
	if err := sink.Emit(ev); err != nil { // 重放：幂等跳过
		t.Fatal(err)
	}
	if err := sink.Flush(); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	if len(lines) != 1 {
		t.Fatalf("应只输出 1 行（重放被吸收），实际 %d: %q", len(lines), buf.String())
	}
	for _, key := range []string{`"version":3`, `"id":"e1"`, `"symbol":"S"`, `"price":1.5`,
		`"volume":2`, `"seq":1`, `"event_time":"2026-10-06T10:00:01Z"`,
		`"exchange":"COINBASE"`, `"venue_mic":"COIN"`, `"is_maker":true`,
		`"at":"2026-10-06T10:00:01Z"`} {
		if !strings.Contains(lines[0], key) {
			t.Fatalf("输出缺少字段 %s: %s", key, lines[0])
		}
	}
	if sink.Written() != 1 {
		t.Fatalf("Written = %d, 期望 1", sink.Written())
	}
	t.Logf("用例结果: 核心字段+4 个已声明扩展字段（含时间类型）全部输出，重放被幂等吸收 [通过]\n  %s", lines[0])
}

// TestLineSourceOversizeLineBounded 验证超长行整条记为坏行、字节位点连续、
// 后续行仍能正常读取（单行异常不拖垮整条流，且不把超大行完整留在内存）。
func TestLineSourceOversizeLineBounded(t *testing.T) {
	t.Log("业务场景: 一行超过最大行长被整条拒绝（坏行），其后的正常行不受影响")
	good := `{"version":1,"payload":{"id":"after"}}`
	huge := strings.Repeat("x", maxLineLen+10)
	var sb strings.Builder
	sb.WriteString(huge)
	sb.WriteByte('\n')
	sb.WriteString(good)
	sb.WriteByte('\n')

	src := NewLineSource(strings.NewReader(sb.String()))
	// 超长行在同一次读取中被记为坏行并跳过，Next 继续返回后面的正常行。
	rec, ok := src.Next()
	if !ok || rec.Payload["id"] != "after" {
		t.Fatalf("超长行应被跳过并读到 after, 得到 %+v ok=%v", rec, ok)
	}
	bad := src.BadLines()
	if len(bad) != 1 || bad[0].Line != 1 || len(bad[0].Raw) > maxBadLinePreview {
		t.Fatalf("超长坏行记录不符合预期: %+v", bad)
	}
	if _, ok := src.Next(); ok {
		t.Fatal("之后应耗尽")
	}
	t.Logf("用例结果: 超长行(>%d)整条记坏行且仅留 %d 字节预览，后续行正常 [通过]",
		maxLineLen, len(bad[0].Raw))
}

// TestLineSourceChunkedValidLine 验证比内部读缓冲长、但未超业务上限的合法行
// 能被分块拼回并正确解析。
func TestLineSourceChunkedValidLine(t *testing.T) {
	t.Log("业务场景: 单条合法行长于 64KB 读缓冲，仍可分块拼回解析，位点正确")
	// 构造一个超长但合法的平铺 JSON：在 symbol 里填充大量被忽略空格不行，
	// 改为塞入一个合法的长字符串字段（symbol 允许长字符串）。
	longSymbol := strings.Repeat("S", 100000) // 100KB，超过 64KB 读缓冲
	line := `{"version":1,"id":"big","symbol":"` + longSymbol + `","price":1,"volume":1,"seq":1,"event_time":"2026-10-06T10:00:01Z"}`
	src := NewLineSource(strings.NewReader(line + "\n"))
	rec, ok := src.Next()
	if !ok {
		t.Fatal("应读到长合法行")
	}
	if rec.Payload["symbol"].(string) != longSymbol {
		t.Fatal("长行内容拼回不完整")
	}
	if rec.NextOffset != int64(len(line)+1) {
		t.Fatalf("NextOffset=%d, 期望 %d", rec.NextOffset, len(line)+1)
	}
	if _, ok := src.Next(); ok {
		t.Fatal("之后应耗尽")
	}
	t.Logf("用例结果: %d 字节的合法行分块拼回解析成功，位点=%d [通过]", len(line), rec.NextOffset)
}
