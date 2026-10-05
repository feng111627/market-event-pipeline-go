package schema

import (
	"errors"
	"strings"
	"testing"
)

// regV1V2 构造：
//
//	v1: 无 extra
//	v2: 新增可选字段 bid_size（缺省 0）
func regV1V2(t *testing.T) *Registry {
	t.Helper()
	r := NewRegistry()
	if err := r.Register(1, nil); err != nil {
		t.Fatalf("register v1: %v", err)
	}
	if err := r.Register(2, []FieldDef{
		{Name: "bid_size", Kind: KindInt64, Required: false, Default: 0, HasDefault: true},
	}); err != nil {
		t.Fatalf("register v2: %v", err)
	}
	return r
}

func TestSchema_AdditiveEvolutionWithStableDefault(t *testing.T) {
	r := regV1V2(t)
	logCase(t, "新增字段带缺省值：旧版本记录按缺省物化，语义稳定可复现")

	// 一条 v1 记录（没有 extra），用 v2 规则解码时缺失字段必须取缺省 0
	rec, err := r.Decode(`{"schema_v":1,"seq_id":"a","symbol":"X","price":10,"qty":1,"event_time":"2026-10-06T10:00:00Z"}`)
	if err != nil {
		t.Fatalf("decode v1: %v", err)
	}
	if got := rec.Fields["bid_size"]; got != 0 {
		t.Fatalf("sticky default want 0, got %d", got)
	}
	// 再解一次，结果必须一致（可复现）
	rec2, _ := r.Decode(`{"schema_v":1,"seq_id":"a","symbol":"X","price":10,"qty":1,"event_time":"2026-10-06T10:00:00Z"}`)
	if rec2.Fields["bid_size"] != rec.Fields["bid_size"] {
		t.Fatalf("default materialization not reproducible")
	}
	pass(t, "旧记录缺省 bid_size=0，两次解码一致")

	// v2 记录显式带值则取实际值
	rec3, err := r.Decode(`{"schema_v":2,"seq_id":"b","symbol":"X","price":10,"qty":1,"event_time":"2026-10-06T10:00:00Z","extra":{"bid_size":42}}`)
	if err != nil {
		t.Fatalf("decode v2: %v", err)
	}
	if rec3.Fields["bid_size"] != 42 {
		t.Fatalf("explicit value want 42, got %d", rec3.Fields["bid_size"])
	}
	pass(t, "v2 显式 bid_size=42 正确解析")
}

func TestSchema_IncompatibleChangesRejected(t *testing.T) {
	logCase(t, "不兼容结构变更必须在注册阶段被明确拒绝")
	af := func(name string, def int64, hasDef bool, req bool) FieldDef {
		return FieldDef{Name: name, Kind: KindInt64, Required: req, Default: def, HasDefault: hasDef}
	}

	type step struct {
		v      int
		fields []FieldDef
	}
	cases := []struct {
		name  string
		steps []step
		kw    string
	}{
		{"删除字段", []step{
			{1, []FieldDef{af("a", 0, true, false)}},
			{2, nil},
		}, "removed"},
		{"改字段类型", []step{
			{1, []FieldDef{af("a", 0, true, false)}},
			{2, []FieldDef{{Name: "a", Kind: "bogus", Required: false, Default: 0, HasDefault: true}}},
		}, "kind"},
		{"新增字段不给缺省值", []step{
			{1, []FieldDef{af("a", 0, true, false)}},
			{2, []FieldDef{af("a", 0, true, false), af("b", 0, false, false)}},
		}, "default"},
		{"缺省值被改动", []step{
			{1, []FieldDef{af("a", 0, true, false)}},
			{2, []FieldDef{af("a", 9, true, false)}},
		}, "default changed"},
		{"必填改可选", []step{
			{1, []FieldDef{af("a", 0, true, false)}},
			{2, []FieldDef{af("a", 0, true, true)}},  // 可选 -> 必填，允许（需缺省）
			{3, []FieldDef{af("a", 0, true, false)}}, // 必填 -> 可选，拒绝
		}, "required->optional"},
		{"重复注册同一版本", []step{{1, nil}, {1, nil}}, "already registered"},
		{"版本号跳跃", []step{{1, nil}, {3, nil}}, "increment by 1"},
	}

	for _, c := range cases {
		r := NewRegistry()
		var err error
		for i, st := range c.steps {
			err = r.Register(st.v, st.fields)
			if err != nil {
				if i != len(c.steps)-1 {
					t.Fatalf("[%s] unexpected failure at setup step %d: %v", c.name, i, err)
				}
				break
			}
		}
		if err == nil {
			t.Fatalf("[%s] expected rejection, got nil", c.name)
		}
		if !errors.Is(err, ErrIncompatible) {
			t.Fatalf("[%s] want ErrIncompatible, got %v", c.name, err)
		}
		if !strings.Contains(err.Error(), c.kw) {
			t.Fatalf("[%s] err=%q want contains %q", c.name, err.Error(), c.kw)
		}
		pass(t, c.name+" -> 拒绝: "+err.Error())
	}
}

func TestSchema_UnknownVersionAndUndeclaredFieldRejected(t *testing.T) {
	r := regV1V2(t)
	logCase(t, "未注册版本与未声明字段必须被拒绝，不能带错误语义继续")

	if _, err := r.Decode(`{"schema_v":99,"seq_id":"a","symbol":"X","price":1,"qty":1,"event_time":"2026-10-06T10:00:00Z"}`); !errors.Is(err, ErrUnknownVersion) {
		t.Fatalf("want ErrUnknownVersion, got %v", err)
	}
	pass(t, "schema_v=99 -> ErrUnknownVersion")

	if _, err := r.Decode(`{"schema_v":1,"seq_id":"a","symbol":"X","price":1,"qty":1,"event_time":"2026-10-06T10:00:00Z","extra":{"ghost":1}}`); err == nil {
		t.Fatalf("undeclared field must be rejected")
	} else if !errors.Is(err, ErrIncompatible) {
		t.Fatalf("want ErrIncompatible for undeclared field, got %v", err)
	}
	pass(t, "v1 携带未声明 extra.ghost -> ErrIncompatible")

	for _, bad := range []string{
		`{"schema_v":1,"symbol":"X","price":1,"qty":1,"event_time":"2026-10-06T10:00:00Z"}`, // 缺 seq_id
		`{"schema_v":1,"seq_id":"a","symbol":"X","price":1,"qty":1,"event_time":"not-a-time"}`,
		`{not json`,
	} {
		if _, err := r.Decode(bad); err == nil {
			t.Fatalf("bad record must error: %s", bad)
		}
	}
	pass(t, "缺核心字段/坏时间/坏 JSON 全部拒绝")
}

func mustOK(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func logCase(t *testing.T, name string) {
	t.Helper()
	t.Logf("【业务覆盖】%s", name)
}

func pass(t *testing.T, msg string) {
	t.Helper()
	t.Logf("  ✔ %s", msg)
}
