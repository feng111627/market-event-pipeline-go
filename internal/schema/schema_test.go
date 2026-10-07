package schema

import (
	"testing"
	"time"
)

// TestNormalize_Defaults 验证可选字段缺失时缺省值语义稳定可复现。
func TestNormalize_Defaults(t *testing.T) {
	t.Log("业务场景: 结构演进后上游新增可选字段，旧数据缺失该字段时应填充稳定缺省值")
	s := Schema{
		Version: 2,
		Fields: []Field{
			{Name: "id", Type: TypeString, Required: true},
			{Name: "price", Type: TypeFloat, Required: true},
			{Name: "exchange", Type: TypeString, Required: false, Default: "UNKNOWN"},
			{Name: "flags", Type: TypeInt, Required: false}, // 未声明缺省 -> 类型零值
		},
	}
	rec := map[string]interface{}{"id": "e1", "price": 10.5}
	out1, err := s.Normalize(rec)
	if err != nil {
		t.Fatalf("Normalize 失败: %v", err)
	}
	out2, _ := s.Normalize(rec)
	if out1["exchange"] != "UNKNOWN" {
		t.Errorf("exchange 缺省值 = %v, 期望 UNKNOWN", out1["exchange"])
	}
	if out1["flags"] != int64(0) {
		t.Errorf("flags 缺省值 = %v, 期望类型零值 0", out1["flags"])
	}
	if out1["exchange"] != out2["exchange"] || out1["flags"] != out2["flags"] {
		t.Error("两次 Normalize 缺省值不一致，缺省语义不可复现")
	}
	t.Logf("用例结果: 缺省值填充稳定可复现 exchange=%v flags=%v [通过]", out1["exchange"], out1["flags"])
}

// TestNormalize_RequiredMissing 验证必填字段缺失被明确拒绝。
func TestNormalize_RequiredMissing(t *testing.T) {
	t.Log("业务场景: 上游记录缺少必填字段，必须拒绝而不是按零值继续")
	s := Schema{Version: 1, Fields: []Field{{Name: "id", Type: TypeString, Required: true}}}
	if _, err := s.Normalize(map[string]interface{}{}); err == nil {
		t.Fatal("缺少必填字段未报错")
	} else {
		t.Logf("用例结果: 必填缺失被拒绝 err=%v [通过]", err)
	}
}

// TestNormalize_UnknownFieldIgnored 验证未知字段被忽略（向前兼容读取）。
func TestNormalize_UnknownFieldIgnored(t *testing.T) {
	t.Log("业务场景: 上游先发了本地未注册的新字段，应忽略而不是报错")
	s := Schema{Version: 1, Fields: []Field{{Name: "id", Type: TypeString, Required: true}}}
	out, err := s.Normalize(map[string]interface{}{"id": "e1", "brand_new_field": "x"})
	if err != nil {
		t.Fatalf("未知字段导致报错: %v", err)
	}
	if _, ok := out["brand_new_field"]; ok {
		t.Error("未知字段不应进入清洗结果")
	}
	t.Log("用例结果: 未知字段被安全忽略 [通过]")
}

// TestNormalize_BadType 验证类型不匹配被拒绝。
func TestNormalize_BadType(t *testing.T) {
	t.Log("业务场景: 字段值类型与结构声明不符，必须拒绝")
	s := Schema{Version: 1, Fields: []Field{{Name: "price", Type: TypeFloat, Required: true}}}
	if _, err := s.Normalize(map[string]interface{}{"price": "not-a-number"}); err == nil {
		t.Fatal("类型不匹配未报错")
	} else {
		t.Logf("用例结果: 类型不匹配被拒绝 err=%v [通过]", err)
	}
}

// TestNormalize_TimeCoerce 验证时间字段的约定转换（RFC3339 字符串 / Unix 毫秒）。
func TestNormalize_TimeCoerce(t *testing.T) {
	t.Log("业务场景: 事件时间以字符串或毫秒时间戳到达，应按约定转换")
	s := Schema{Version: 1, Fields: []Field{{Name: "ts", Type: TypeTime, Required: true}}}
	want := time.Date(2026, 10, 6, 10, 0, 0, 0, time.UTC)
	out, err := s.Normalize(map[string]interface{}{"ts": "2026-10-06T10:00:00Z"})
	if err != nil {
		t.Fatalf("RFC3339 转换失败: %v", err)
	}
	if !out["ts"].(time.Time).Equal(want) {
		t.Errorf("时间转换结果错误: %v", out["ts"])
	}
	out2, err := s.Normalize(map[string]interface{}{"ts": want.UnixMilli()})
	if err != nil || !out2["ts"].(time.Time).Equal(out["ts"].(time.Time)) {
		t.Fatalf("毫秒时间戳转换不一致: %v %v", out2["ts"], err)
	}
	// JSON 数字解码为 float64：整数毫秒必须可无损接受（外部 JSONL 的主要形态）。
	out3, err := s.Normalize(map[string]interface{}{"ts": float64(want.UnixMilli())})
	if err != nil || !out3["ts"].(time.Time).Equal(out["ts"].(time.Time)) {
		t.Fatalf("float64 毫秒时间戳转换不一致: %v %v", out3["ts"], err)
	}
	// 非整数毫秒无法无损表达，明确拒绝。
	if _, err := s.Normalize(map[string]interface{}{"ts": 1.5}); err == nil {
		t.Fatal("非整数毫秒时间戳应被拒绝")
	}
	t.Log("用例结果: 字符串、整数毫秒与 JSON 数字毫秒转换为同一事件时间，小数毫秒拒绝 [通过]")
}

// TestCompatibility 覆盖演进规则：允许新增可选字段，拒绝破坏性变更。
func TestCompatibility(t *testing.T) {
	t.Log("业务场景: 结构演进兼容性校验")
	v1 := Schema{Version: 1, Fields: []Field{
		{Name: "id", Type: TypeString, Required: true},
		{Name: "tag", Type: TypeString, Required: false},
	}}
	cases := []struct {
		name    string
		evolve  Schema
		wantErr bool
	}{
		{"新增带缺省的可选字段(兼容)", Schema{Version: 2, Fields: append(append([]Field{}, v1.Fields...), Field{Name: "extra", Type: TypeString, Required: false, Default: "d"})}, false},
		{"删除字段(不兼容)", Schema{Version: 2, Fields: v1.Fields[:1]}, true},
		{"修改字段类型(不兼容)", Schema{Version: 2, Fields: []Field{{Name: "id", Type: TypeInt, Required: true}, {Name: "tag", Type: TypeString, Required: false}}}, true},
		{"可选改必填(不兼容)", Schema{Version: 2, Fields: []Field{{Name: "id", Type: TypeString, Required: true}, {Name: "tag", Type: TypeString, Required: true}}}, true},
		{"新增必填字段(不兼容)", Schema{Version: 2, Fields: append(append([]Field{}, v1.Fields...), Field{Name: "must", Type: TypeString, Required: true})}, true},
	}
	for _, c := range cases {
		err := CheckCompatibility(v1, c.evolve)
		if (err != nil) != c.wantErr {
			t.Errorf("%s: err=%v, wantErr=%v", c.name, err, c.wantErr)
			continue
		}
		t.Logf("用例结果: %s -> err=%v [通过]", c.name, err)
	}
}

// TestRegistry_RejectsIncompatible 验证注册表拒绝不兼容演进、接受兼容演进。
func TestRegistry_RejectsIncompatible(t *testing.T) {
	t.Log("业务场景: 注册新结构版本时强制兼容校验")
	r := NewRegistry()
	if err := r.Register(MarketV1ForTest()); err != nil {
		t.Fatalf("注册 v1 失败: %v", err)
	}
	good := MarketV1ForTest()
	good.Version = 2
	good.Fields = append(good.Fields, Field{Name: "exchange", Type: TypeString, Required: false, Default: "UNKNOWN"})
	if err := r.Register(good); err != nil {
		t.Fatalf("兼容演进被拒绝: %v", err)
	}
	bad := MarketV1ForTest()
	bad.Version = 3
	bad.Fields = bad.Fields[:1] // 删除字段
	if err := r.Register(bad); err == nil {
		t.Fatal("不兼容演进未被拒绝")
	} else {
		t.Logf("用例结果: 不兼容演进被拒绝 err=%v [通过]", err)
	}
	latest, _ := r.Latest()
	if latest.Version != 2 {
		t.Errorf("最新版本 = %d, 期望 2（被拒绝的 v3 不应生效）", latest.Version)
	}
	t.Log("用例结果: 注册表只接受兼容演进，拒绝后最新版本保持 v2 [通过]")
}

// MarketV1ForTest 构造测试用 v1 结构。
func MarketV1ForTest() Schema {
	return Schema{Version: 1, Fields: []Field{
		{Name: "id", Type: TypeString, Required: true},
		{Name: "price", Type: TypeFloat, Required: true},
	}}
}
