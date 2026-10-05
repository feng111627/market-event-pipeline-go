// Package schema 定义行情事件的结构描述、演进兼容性规则与缺省值语义。
package schema

import "fmt"

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

// Registry 按版本保存结构，并负责演进校验。
type Registry struct {
	schemas map[int]Schema
	latest  int
}

// NewRegistry 创建空注册表。
func NewRegistry() *Registry {
	return &Registry{schemas: make(map[int]Schema)}
}

// Register 注册新版本结构；与上一版本不兼容时返回错误并拒绝注册。
func (r *Registry) Register(s Schema) error {
	return fmt.Errorf("not implemented")
}

// Latest 返回最新版本结构。
func (r *Registry) Latest() (Schema, bool) {
	return Schema{}, false
}

// CheckCompatibility 校验从 old 演进到 new 是否兼容。
// 规则：只允许新增带缺省值的可选字段；删除字段、改类型、把可选改为必填均不兼容。
func CheckCompatibility(old, new Schema) error {
	return fmt.Errorf("not implemented")
}

// Normalize 按结构清洗一条原始记录：
// 缺省值填充、未知字段忽略、必填缺失或不兼容类型返回错误。
func (s Schema) Normalize(record map[string]interface{}) (map[string]interface{}, error) {
	return nil, fmt.Errorf("not implemented")
}
