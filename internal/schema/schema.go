// Package schema 定义行情事件的结构描述、演进兼容性规则与缺省值语义。
//
// 演进规则（向新版本注册时强制校验）：
//   - 允许：新增带缺省值的可选字段（旧数据用缺省值补齐，语义稳定可复现）。
//   - 拒绝：删除字段、修改字段类型、把已有可选字段改为必填、新增必填字段。
//
// 未知字段（上游先发了新字段而本地结构未注册）一律忽略，保证向前兼容读取。
package schema

import (
	"fmt"
	"time"
)

// FieldType 是字段的逻辑类型。
type FieldType string

const (
	TypeString FieldType = "string"
	TypeFloat  FieldType = "float"
	TypeInt    FieldType = "int"
	TypeTime   FieldType = "time"
	TypeBool   FieldType = "bool"
)

// Field 描述一个字段。
type Field struct {
	Name     string
	Type     FieldType
	Required bool        // 必填字段缺失时拒绝；可选字段缺失时取缺省值
	Default  interface{} // 可选字段的缺省值，nil 表示该类型的零值
}

// Schema 是某一版本的事件结构。
type Schema struct {
	Version int
	Fields  []Field
}

// field 按名查找字段。
func (s Schema) field(name string) (Field, bool) {
	for _, f := range s.Fields {
		if f.Name == name {
			return f, true
		}
	}
	return Field{}, false
}

// Registry 按版本保存结构，并负责演进校验。
type Registry struct {
	schemas map[int]Schema
	latest  int
	hasAny  bool
}

// NewRegistry 创建空注册表。
func NewRegistry() *Registry {
	return &Registry{schemas: make(map[int]Schema)}
}

// Register 注册新版本结构；与上一版本不兼容时返回错误并拒绝注册。
func (r *Registry) Register(s Schema) error {
	if _, dup := r.schemas[s.Version]; dup {
		return fmt.Errorf("schema version %d already registered", s.Version)
	}
	if r.hasAny {
		if s.Version <= r.latest {
			return fmt.Errorf("schema version %d must be greater than latest %d", s.Version, r.latest)
		}
		if err := CheckCompatibility(r.schemas[r.latest], s); err != nil {
			return fmt.Errorf("incompatible schema evolution v%d -> v%d: %w", r.latest, s.Version, err)
		}
	}
	r.schemas[s.Version] = s
	if !r.hasAny || s.Version > r.latest {
		r.latest = s.Version
		r.hasAny = true
	}
	return nil
}

// Latest 返回最新版本结构。
func (r *Registry) Latest() (Schema, bool) {
	if !r.hasAny {
		return Schema{}, false
	}
	return r.schemas[r.latest], true
}

// Get 返回指定版本结构。
func (r *Registry) Get(version int) (Schema, bool) {
	s, ok := r.schemas[version]
	return s, ok
}

// CheckCompatibility 校验从 old 演进到 new 是否兼容。
func CheckCompatibility(old, new Schema) error {
	for _, of := range old.Fields {
		nf, ok := new.field(of.Name)
		if !ok {
			return fmt.Errorf("field %q removed", of.Name)
		}
		if nf.Type != of.Type {
			return fmt.Errorf("field %q type changed: %s -> %s", of.Name, of.Type, nf.Type)
		}
		if !of.Required && nf.Required {
			return fmt.Errorf("field %q changed from optional to required", of.Name)
		}
	}
	for _, nf := range new.Fields {
		if _, ok := old.field(nf.Name); ok {
			continue
		}
		if nf.Required {
			return fmt.Errorf("new field %q must be optional with a default", nf.Name)
		}
	}
	return nil
}

// Normalize 按结构清洗一条原始记录：
//   - 必填字段缺失：返回错误（拒绝）。
//   - 可选字段缺失：填充声明的缺省值（未声明则用类型零值），语义稳定可复现。
//   - 字段值类型不匹配且无法转换：返回错误（拒绝）。
//   - 记录中的未知字段：忽略，不进入结果。
func (s Schema) Normalize(record map[string]interface{}) (map[string]interface{}, error) {
	out := make(map[string]interface{}, len(s.Fields))
	for _, f := range s.Fields {
		raw, present := record[f.Name]
		if !present || raw == nil {
			if f.Required {
				return nil, fmt.Errorf("required field %q missing", f.Name)
			}
			out[f.Name] = defaultValue(f)
			continue
		}
		v, err := coerce(f, raw)
		if err != nil {
			return nil, err
		}
		out[f.Name] = v
	}
	return out, nil
}

// defaultValue 返回字段的缺省值；未声明时回退到类型零值。
func defaultValue(f Field) interface{} {
	if f.Default != nil {
		return f.Default
	}
	switch f.Type {
	case TypeString:
		return ""
	case TypeFloat:
		return float64(0)
	case TypeInt:
		return int64(0)
	case TypeBool:
		return false
	case TypeTime:
		return time.Time{}
	default:
		return nil
	}
}

// coerce 把原始值转换到字段声明的类型；只允许无损或约定明确的转换。
func coerce(f Field, raw interface{}) (interface{}, error) {
	switch f.Type {
	case TypeString:
		if v, ok := raw.(string); ok {
			return v, nil
		}
	case TypeFloat:
		switch v := raw.(type) {
		case float64:
			return v, nil
		case float32:
			return float64(v), nil
		case int:
			return float64(v), nil
		case int64:
			return float64(v), nil
		}
	case TypeInt:
		switch v := raw.(type) {
		case int:
			return int64(v), nil
		case int64:
			return v, nil
		case float64:
			if v == float64(int64(v)) {
				return int64(v), nil
			}
		}
	case TypeBool:
		if v, ok := raw.(bool); ok {
			return v, nil
		}
	case TypeTime:
		switch v := raw.(type) {
		case time.Time:
			return v, nil
		case string:
			if ts, err := time.Parse(time.RFC3339Nano, v); err == nil {
				return ts, nil
			}
		case int64: // Unix 毫秒
			return time.UnixMilli(v), nil
		case int: // 内存构造的记录
			return time.UnixMilli(int64(v)), nil
		case float64: // JSON 数字解码为 float64；整数毫秒才可无损接受
			if v == float64(int64(v)) {
				return time.UnixMilli(int64(v)), nil
			}
		}
	}
	return nil, fmt.Errorf("field %q: value %v (%T) not assignable to %s", f.Name, raw, raw, f.Type)
}
