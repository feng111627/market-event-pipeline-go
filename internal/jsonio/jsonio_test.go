package jsonio

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/feng111627/market-event-pipeline-go/internal/event"
	"github.com/feng111627/market-event-pipeline-go/internal/pipeline"
)

func parseOne(t *testing.T, line string) pipeline.SourceRecord {
	t.Helper()
	lr := NewLineReader(strings.NewReader(line+"\n"), 0)
	rec, ok := lr.ReadLine()
	if !ok {
		t.Fatalf("行未被读取: %q", line)
	}
	return rec
}

// TestParseLineShapes 覆盖两种行格式（包装写法/扁平写法）与默认版本。
func TestParseLineShapes(t *testing.T) {
	t.Log("业务场景: 每行带结构版本，支持 {version,data} 包装与扁平两种写法，缺省版本按 1")
	cases := []struct {
		name    string
		line    string
		version int
		id      string
	}{
		{"包装写法", `{"version":2,"data":{"id":"a"}}`, 2, "a"},
		{"扁平写法", `{"version":3,"id":"b"}`, 3, "b"},
		{"缺省版本", `{"id":"c"}`, 1, "c"},
	}
	for _, c := range cases {
		rec := parseOne(t, c.line)
		if rec.ParseError != "" {
			t.Fatalf("%s: 解析失败: %s", c.name, rec.ParseError)
		}
		if rec.SchemaVersion != c.version {
			t.Errorf("%s: version=%d 期望 %d", c.name, rec.SchemaVersion, c.version)
		}
		if rec.Payload["id"] != c.id {
			t.Errorf("%s: id=%v 期望 %s", c.name, rec.Payload["id"], c.id)
		}
		t.Logf("用例结果: %s -> version=%d id=%v [通过]", c.name, rec.SchemaVersion, rec.Payload["id"])
	}

	// 非法 JSON 与空行应产出 ParseError 而非中断整批。
	for _, bad := range []string{"not-json", "   "} {
		rec := parseOne(t, bad)
		if rec.ParseError == "" {
			t.Fatalf("行 %q 应被标记 ParseError", bad)
		}
	}
	t.Log("用例结果: 非法 JSON / 空行标记为 ParseError（由流水线计 REJECTED，不中断整批） [通过]")
}

// TestFileSourceOffsets 验证字节位点随读取推进，SeekTo 可回到指定位点。
func TestFileSourceOffsets(t *testing.T) {
	t.Log("业务场景: 断点恢复需要源支持字节位点定位")
	path := filepath.Join(t.TempDir(), "in.jsonl")
	content := "{\"id\":\"a\"}\n{\"id\":\"b\"}\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	src, err := OpenFileSource(path)
	if err != nil {
		t.Fatal(err)
	}
	r1, ok := src.Next()
	if !ok {
		t.Fatal("应读到第一行")
	}
	if r1.Payload["id"] != "a" || r1.Offset != 0 {
		t.Fatalf("第一行异常: %+v", r1)
	}
	r2, _ := src.Next()
	if r2.Payload["id"] != "b" {
		t.Fatalf("第二行 id=%v", r2.Payload["id"])
	}
	// 回到第二行起始位点，应重新读到 b。
	if err := src.SeekTo(r2.Offset); err != nil {
		t.Fatalf("SeekTo 失败: %v", err)
	}
	r2b, _ := src.Next()
	if r2b.Payload["id"] != "b" {
		t.Fatalf("Seek 后应重读第二行, 得到 %v", r2b.Payload["id"])
	}
	t.Logf("用例结果: 第二行位点=%d，SeekTo 后从第二行重读成功 [通过]", r2.Offset)
}

// TestFileSinkSyncTruncate 验证持久化 sink 的位点推进与截断恢复。
func TestFileSinkSyncTruncate(t *testing.T) {
	t.Log("业务场景: 输出先缓冲，Sync 后落盘并推进位点；恢复时 Truncate 丢弃未落盘部分")
	path := filepath.Join(t.TempDir(), "out.jsonl")
	sink, err := OpenFileSink(path)
	if err != nil {
		t.Fatal(err)
	}
	ev := mkEv("e1", 2, nil)
	for i := 0; i < 3; i++ {
		if err := sink.Emit(ev); err != nil {
			t.Fatal(err)
		}
	}
	if off := sink.DurableOffset(); off != 0 {
		t.Fatalf("Sync 前位点应为 0, 得到 %d", off)
	}
	if err := sink.Sync(); err != nil {
		t.Fatal(err)
	}
	committed := sink.DurableOffset()
	if committed <= 0 {
		t.Fatalf("Sync 后位点应 >0, 得到 %d", committed)
	}
	// 再写两条但不 Sync（模拟崩溃时已缓冲/已写未入断点）。
	if err := sink.Emit(ev); err != nil {
		t.Fatal(err)
	}
	if err := sink.Emit(ev); err != nil {
		t.Fatal(err)
	}
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
	sink2, err := OpenFileSink(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := sink2.Truncate(committed); err != nil {
		t.Fatalf("Truncate 失败: %v", err)
	}
	if err := sink2.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(data)) != committed {
		t.Fatalf("截断后文件大小=%d 期望 %d", len(data), committed)
	}
	t.Logf("用例结果: Sync 推进位点到 %d，Truncate 把 5 行回滚为 3 行 [通过]", committed)
}

// TestOutputRecordFlat 验证输出为扁平 JSON：核心字段 + 全部扩展字段。
func TestOutputRecordFlat(t *testing.T) {
	t.Log("业务场景: 下游应在扁平输出中拿到核心字段与全部已声明扩展字段")
	ev := mkEv("e1", 3, map[string]interface{}{
		"exchange": "BINANCE", "is_maker": true, "venue_id": int64(9),
	})
	out, err := json.Marshal(ToOutputRecord(3, ev))
	if err != nil {
		t.Fatal(err)
	}
	s := string(out)
	for _, key := range []string{`"schema_version":3`, `"id":"e1"`, `"exchange":"BINANCE"`,
		`"is_maker":true`, `"venue_id":9`, `"event_time":"2026-10-06T10:00:00Z"`} {
		if !strings.Contains(s, key) {
			t.Errorf("输出缺少 %s: %s", key, s)
		}
	}
	t.Logf("用例结果: 扁平输出含全部字段且 event_time 为 RFC3339 [通过] %s", s)
}

func mkEv(id string, version int, attrs map[string]interface{}) event.MarketEvent {
	ev := event.MarketEvent{ID: id, Symbol: "S", Price: 1, Volume: 2, Seq: 1,
		EventTime: time.Date(2026, 10, 6, 10, 0, 0, 0, time.UTC), Attrs: attrs}
	ev.SetSchemaVersion(version)
	return ev
}
