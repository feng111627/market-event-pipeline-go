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
//
// 核心字段是 V1 起就存在的固定字段；Attrs 承载该事件所属结构版本中
// **已声明**的其余字段（含后续兼容演进新增的可选字段，以及为其补齐的
// 稳定缺省值）。结构中未声明的未知字段不会进入 Attrs，在清洗阶段即被忽略。
// 下游因此可以拿到所有已声明字段，而不会只看到几个核心字段。
type MarketEvent struct {
	ID        string                 // 全局唯一事件 ID（去重键）
	Symbol    string                 // 标的代码
	Price     float64                // 成交价
	Volume    float64                // 成交量
	Seq       uint64                 // 同一 ID 的版本序号，用于更正合并
	EventTime time.Time              // 事件时间（业务发生时间）
	SchemaVer int                    // 该事件按哪个结构版本清洗（0 表示未设置）
	Attrs     map[string]interface{} // 已声明的非核心字段（含缺省值），键为字段名
}

// Result 是单条事件的处理回执。
type Result struct {
	EventID     string
	Disposition Disposition
	Reason      string
}
