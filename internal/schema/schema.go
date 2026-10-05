// Package schema 负责行情数据结构的版本管理与兼容判定。
//
// 兼容规则（只允许“加法且带缺省值”的演进）：
//   - 新增可选字段：必须声明缺省值（sticky 缺省：记录到达时物化，缺省语义因此稳定可复现）；
//   - 可选字段改为必填：允许，但必须为旧版本/未携带该字段的记录声明缺省值；
//   - 其余变更（删字段、改类型、改字段名、改语义、必填改可选、改核心字段）一律拒绝（ErrIncompatible）；
//   - 未注册版本、无法解析、缺少必填核心字段的记录一律拒绝（ErrUnknownVersion / ParseError）。
package schema

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

// FieldKind 字段类型。
type FieldKind string

const (
	KindInt64 FieldKind = "int64"
)

// 核心字段由事件结构本身承载，不属于可演进 Extra 字段集合。
var coreFields = map[string]bool{
	"schema_v": true, "seq_id": true, "symbol": true,
	"price": true, "qty": true, "event_time": true,
}

// FieldDef 描述一个演进字段（Extra 中的字段）。
type FieldDef struct {
	Name       string    `json:"name"`
	Kind       FieldKind `json:"kind"`
	Required   bool      `json:"required"`
	Default    int64     `json:"default"`
	HasDefault bool      `json:"has_default"`
}

// Version 描述一个 schema 版本。
type Version struct {
	Version int                 `json:"version"`
	Fields  map[string]FieldDef `json:"fields"`
}

// Registry 已注册 schema 的注册表。
type Registry struct {
	versions map[int]*Version
}

// NewRegistry 创建空注册表。
func NewRegistry() *Registry { return &Registry{versions: make(map[int]*Version)} }

// ErrIncompatible 结构变更不兼容，必须明确拒绝。
var ErrIncompatible = errors.New("schema incompatible change")

// ErrUnknownVersion 记录声明了未注册的 schema 版本。
var ErrUnknownVersion = errors.New("unknown schema version")

// Register 注册一个新版本；相对上一版本不兼容时返回包装了 ErrIncompatible 的错误。
// 要求版本号必须按已有最大版本 + 1 递增（防止回退/跳变引入语义错乱）。
func (r *Registry) Register(v int, fields []FieldDef) error {
	if v < 1 {
		return fmt.Errorf("%w: version must be >= 1, got %d", ErrIncompatible, v)
	}
	fm := make(map[string]FieldDef, len(fields))
	var names []string
	for _, f := range fields {
		if f.Name == "" {
			return fmt.Errorf("%w: empty field name in version %d", ErrIncompatible, v)
		}
		if coreFields[f.Name] {
			return fmt.Errorf("%w: %q is a core field and cannot be redefined", ErrIncompatible, f.Name)
		}
		if f.Kind != KindInt64 {
			return fmt.Errorf("%w: field %q unsupported kind %q", ErrIncompatible, f.Name, f.Kind)
		}
		if _, dup := fm[f.Name]; dup {
			return fmt.Errorf("%w: duplicate field %q in version %d", ErrIncompatible, f.Name, v)
		}
		if f.Required && !f.HasDefault {
			return fmt.Errorf("%w: newly required field %q must declare a default for older records", ErrIncompatible, f.Name)
		}
		fm[f.Name] = f
		names = append(names, f.Name)
	}

	maxV := 0
	for k := range r.versions {
		if k > maxV {
			maxV = k
		}
	}
	if _, exists := r.versions[v]; exists {
		return fmt.Errorf("%w: version %d already registered", ErrIncompatible, v)
	}
	if maxV > 0 && v != maxV+1 {
		return fmt.Errorf("%w: version must increment by 1 (latest=%d, got=%d)", ErrIncompatible, maxV, v)
	}

	if maxV > 0 {
		prev := r.versions[maxV]
		// 删除字段：拒绝
		for name := range prev.Fields {
			if _, ok := fm[name]; !ok {
				return fmt.Errorf("%w: field %q removed in version %d", ErrIncompatible, name, v)
			}
		}
		for name, nf := range fm {
			of, ok := prev.Fields[name]
			if !ok {
				// 新增字段：可选且必须带缺省值（必填字段也要有缺省以物化旧记录）
				if !nf.HasDefault {
					return fmt.Errorf("%w: added field %q must declare a default", ErrIncompatible, name)
				}
				continue
			}
			// 已存在字段：类型/名字不能变
			if nf.Kind != of.Kind {
				return fmt.Errorf("%w: field %q kind changed %q->%q", ErrIncompatible, name, of.Kind, nf.Kind)
			}
			// 必填 -> 可选：语义收紧/放宽方向不一致，拒绝
			if of.Required && !nf.Required {
				return fmt.Errorf("%w: field %q changed required->optional", ErrIncompatible, name)
			}
			// 缺省值不允许改变（否则旧记录的物化结果不可复现）
			if of.HasDefault && nf.HasDefault && of.Default != nf.Default {
				return fmt.Errorf("%w: field %q default changed %d->%d", ErrIncompatible, name, of.Default, nf.Default)
			}
			if of.HasDefault != nf.HasDefault {
				return fmt.Errorf("%w: field %q default presence changed", ErrIncompatible, name)
			}
		}
	}

	sort.Strings(names)
	_ = names
	r.versions[v] = &Version{Version: v, Fields: fm}
	return nil
}

// Get 查询已注册版本。
func (r *Registry) Get(v int) (*Version, bool) {
	ver, ok := r.versions[v]
	return ver, ok
}

// Latest 返回最大已注册版本号。
func (r *Registry) Latest() int {
	maxV := 0
	for k := range r.versions {
		if k > maxV {
			maxV = k
		}
	}
	return maxV
}

// DecodedRecord 解析后的记录。
type DecodedRecord struct {
	SchemaV   int
	SeqID     string
	Symbol    string
	Price     int64
	Qty       int64
	EventTime time.Time
	Fields    map[string]int64 // 全部已知 Extra 字段（含按缺省补全）
	Present   map[string]bool  // 原始记录中实际出现的 Extra 字段
}

// wireRecord 是上游 JSON 线格式。
type wireRecord struct {
	SchemaV   *int             `json:"schema_v"`
	SeqID     *string          `json:"seq_id"`
	Symbol    *string          `json:"symbol"`
	Price     *int64           `json:"price"`
	Qty       *int64           `json:"qty"`
	EventTime *string          `json:"event_time"` // RFC3339Nano
	Extra     map[string]int64 `json:"extra"`
}

func rejectf(cause error, format string, args ...any) error {
	return &ParseError{Cause: cause, Detail: fmt.Sprintf(format, args...)}
}

// Decode 按已注册 schema 解析一条 JSON 记录并物化缺省值。
// 不兼容/非法记录返回错误，调用方必须显式记为拒绝，绝不能静默继续。
func (r *Registry) Decode(raw string) (*DecodedRecord, error) {
	var w wireRecord
	if err := json.NewDecoder(strings.NewReader(raw)).Decode(&w); err != nil {
		return nil, rejectf(err, "invalid json")
	}
	if w.SchemaV == nil {
		return nil, rejectf(ErrIncompatible, "missing required core field schema_v")
	}
	ver, ok := r.versions[*w.SchemaV]
	if !ok {
		return nil, rejectf(ErrUnknownVersion, "schema_v=%d", *w.SchemaV)
	}
	if w.SeqID == nil || *w.SeqID == "" {
		return nil, rejectf(ErrIncompatible, "missing required core field seq_id")
	}
	if w.Symbol == nil || *w.Symbol == "" {
		return nil, rejectf(ErrIncompatible, "missing required core field symbol")
	}
	if w.Price == nil {
		return nil, rejectf(ErrIncompatible, "missing required core field price")
	}
	if w.Qty == nil {
		return nil, rejectf(ErrIncompatible, "missing required core field qty")
	}
	if w.EventTime == nil || *w.EventTime == "" {
		return nil, rejectf(ErrIncompatible, "missing required core field event_time")
	}
	et, err := time.Parse(time.RFC3339Nano, *w.EventTime)
	if err != nil {
		return nil, rejectf(err, "bad event_time %q", *w.EventTime)
	}

	fields := make(map[string]int64, len(ver.Fields))
	present := make(map[string]bool)
	// 先按 schema 物化全部缺省值（稳定可复现的缺省语义）
	for name, def := range ver.Fields {
		fields[name] = def.Default
	}
	// 未知字段：加法演进只允许通过注册声明携带；未声明字段可能携带错误语义，拒绝
	for name := range w.Extra {
		def, known := ver.Fields[name]
		if !known {
			return nil, rejectf(ErrIncompatible, "undeclared field %q in schema_v=%d", name, *w.SchemaV)
		}
		_ = def
		present[name] = true
	}
	for name, val := range w.Extra {
		def := ver.Fields[name]
		if def.Required {
			// 必填字段在该版本必须显式出现
			if !present[name] {
				return nil, rejectf(ErrIncompatible, "required field %q missing in schema_v=%d", name, *w.SchemaV)
			}
		}
		fields[name] = val
	}
	for name, def := range ver.Fields {
		if def.Required && !present[name] {
			return nil, rejectf(ErrIncompatible, "required field %q missing in schema_v=%d", name, *w.SchemaV)
		}
	}

	return &DecodedRecord{
		SchemaV:   *w.SchemaV,
		SeqID:     *w.SeqID,
		Symbol:    *w.Symbol,
		Price:     *w.Price,
		Qty:       *w.Qty,
		EventTime: et,
		Fields:    fields,
		Present:   present,
	}, nil
}

// ParseError 描述记录为何被拒绝。
type ParseError struct {
	Cause  error
	Detail string
}

func (e *ParseError) Error() string {
	if e.Detail != "" {
		return e.Detail + ": " + e.Cause.Error()
	}
	return e.Cause.Error()
}

func (e *ParseError) Unwrap() error { return e.Cause }
