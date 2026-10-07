// Package event 定义行情事件的数据模型与处置结论。
package event

import "time"

// Disposition 表示一条事件被流水线处理后的明确结论。
type Disposition int

const (
	// Accepted 事件被接受并进入乱序缓冲。
	Accepted Disposition = iota
	// DuplicateDropped 重复投递（ID 与内容均一致），直接丢弃，不重复计入。
	DuplicateDropped
	// StaleDropped 同一 ID 但序号更旧的过期版本，丢弃。
	StaleDropped
	// Merged 同一 ID 的更正版本（序号更新），合并替换缓冲中的旧版本。
	Merged
	// LateDropped 事件时间已落后于水位线允许的最大迟到范围，丢弃。
	LateDropped
	// Rejected 结构不兼容或校验失败，明确拒绝。
	Rejected
)

// String 返回处置结论的可读描述，用于日志与测试断言。
func (d Disposition) String() string {
	switch d {
	case Accepted:
		return "ACCEPTED"
	case DuplicateDropped:
		return "DUPLICATE_DROPPED"
	case StaleDropped:
		return "STALE_DROPPED"
	case Merged:
		return "MERGED"
	case LateDropped:
		return "LATE_DROPPED"
	case Rejected:
		return "REJECTED"
	default:
		return "UNKNOWN"
	}
}

// MarketEvent 是一条行情事件。
type MarketEvent struct {
	ID        string    // 全局唯一事件 ID（去重键）
	Symbol    string    // 标的代码
	Price     float64   // 成交价
	Volume    float64   // 成交量
	Seq       uint64    // 同一 ID 的版本序号，用于更正合并
	EventTime time.Time // 事件时间（业务发生时间）
	// Attrs 承载该事件对应结构版本中除核心字段外的全部已声明字段
	// （按最新注册结构清洗后的值）。结构未声明的字段不会出现在这里。
	// key 为字段名，value 的 Go 类型由 schema 清洗决定
	// （string/float64/int64/bool/time.Time）。
	Attrs map[string]interface{}
	// schemaVersion 记录该事件所使用的结构版本，供输出标注（内部字段）。
	schemaVersion int
}

// SchemaVersion 返回事件处理时使用的结构版本。
func (m MarketEvent) SchemaVersion() int { return m.schemaVersion }

// SetSchemaVersion 由流水线在清洗后设置结构版本。
func (m *MarketEvent) SetSchemaVersion(v int) { m.schemaVersion = v }

// Result 是单条事件的处理回执。
type Result struct {
	EventID     string
	Disposition Disposition
	Reason      string
}
