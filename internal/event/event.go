// Package event 定义行情事件、处理结果原因与确定性时钟。
package event

import "time"

// MarketEvent 是规范化后的行情事件。
// Schema 规则保证：新增字段只出现在 Extra 中并带缺省值，核心字段语义不变。
type MarketEvent struct {
	SchemaV   int              `json:"schema_v"`
	SeqID     string           `json:"seq_id"`     // 上游投递幂等键
	Symbol    string           `json:"symbol"`     // 合约代码
	Price     int64            `json:"price"`      // 价格（定点数，单位由 QuoteUnit 决定）
	Qty       int64            `json:"qty"`        // 数量
	EventTime time.Time        `json:"event_time"` // 事件时间（上游产生时间）
	RecvTime  time.Time        `json:"recv_time"`  // 到达时间（本组件打戳）
	Extra     map[string]int64 `json:"extra"`      // 演进字段（仅允许加法演进，均带缺省值）
}

// Source 原始输入行。
type Source struct {
	Offset int64  // 输入流字节偏移（行起始位置，含该行）
	Line   string // 原始一行（JSON 或控制记录）
}

// Reason 用于区分每一种处理结论，绝不允许“静默丢掉”。
type Reason string

const (
	ReasonAccepted         Reason = "accepted"          // 正常输出
	ReasonBuffered         Reason = "buffered"          // 乱序但仍在容忍窗口内，等待
	ReasonDuplicate        Reason = "duplicate"         // 重复投递：seq_id 与内容均一致
	ReasonConflict         Reason = "conflict"          // 重复投递：seq_id 相同但内容不一致，拒绝
	ReasonMerged           Reason = "merged"            // 同 symbol+event_time 的部分字段到达，合并
	ReasonLateDrop         Reason = "late_drop"         // 晚到：event_time 已落后于 watermark，丢弃
	ReasonSchemaRejected   Reason = "schema_rejected"   // 结构不兼容/未注册版本/非法记录，拒绝
	ReasonControlWatermark Reason = "control_watermark" // 输入中的水位控制记录
	ReasonControlRejected  Reason = "control_rejected"  // 控制记录非法（如水位回退），拒绝
	ReasonBackpressure     Reason = "backpressure"      // 下游背压：等待而非丢弃
)

// Decision 是对单条输入的处理结论。
type Decision struct {
	Reason Reason
	Event  *MarketEvent // accepted/merged 时为最终事件
	Detail string       // 人类可读的区分信息
}

// Clock 可注入时钟，便于离线确定性回放与测试。
type Clock interface {
	Now() time.Time
}

// SystemClock 真实墙钟。
type SystemClock struct{}

// Now 返回当前墙钟时间。
func (SystemClock) Now() time.Time { return time.Now() }
