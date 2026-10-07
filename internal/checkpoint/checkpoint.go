// Package checkpoint 提供断点的持久化、恢复与合法性校验。
//
// 语义：
//   - 断点原子写入（临时文件 + rename），崩溃不会留下半截文件。
//   - 断点带有效期（TTL），过期后拒绝恢复（ErrExpired），避免从过旧状态接着算。
//   - 恢复位点早于已提交位点属回退，明确拒绝（ErrRollback）。
package checkpoint

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// ErrExpired 断点已过期，拒绝恢复。
var ErrExpired = errors.New("checkpoint expired")

// ErrRollback 请求的恢复位点早于已提交位点，属回退，拒绝。
var ErrRollback = errors.New("checkpoint rollback rejected")

// State 是一次断点的完整内容。
type State struct {
	Offset       int64                 // 已提交的源位点（下一条待读）
	SinkOffset   int64                 // 已提交且已持久化到 sink 的字节位点（支持输出截断重放）
	Watermark    time.Time             // 已提交的水位线基准（maxSeen）
	EmittedFloor time.Time             // 因缓冲上界强制输出形成的事件时间地板
	HasFloor     bool                  // EmittedFloor 是否有效
	DedupState   map[string]DedupEntry // 去重状态快照
	Buffered     []BufferedEvent       // 已处理但未输出（滞留在重排缓冲）的事件
	Committed    time.Time             // 断点提交时间
	Generation   uint64                // 单调递增的代际号
}

// BufferedEvent 是重排缓冲中事件的可序列化形式。
type BufferedEvent struct {
	SchemaVersion int                    `json:"schema_version,omitempty"`
	ID            string                 `json:"id"`
	Symbol        string                 `json:"symbol"`
	Price         float64                `json:"price"`
	Volume        float64                `json:"volume"`
	Seq           uint64                 `json:"seq"`
	EventTime     time.Time              `json:"event_time"`
	Attrs         map[string]interface{} `json:"attrs,omitempty"` // 全部已声明扩展字段
}

// timeMarkerKey / intMarkerKey 用于在 JSON 中标记带类型的扩展字段。
// Attrs 中只会出现标量（string/float64/int64/bool/time.Time），
// 不可能出现同名嵌套对象，因此这些标记无歧义。
const (
	timeMarkerKey = "__time__"
	intMarkerKey  = "__int__"
)

// MarshalJSON 自定义编码：Attrs 中的 time.Time / int64 用标记对象包裹，
// 保证恢复后下游拿到的值类型不变（语义可复现，int64 不会退化为 float64）。
func (b BufferedEvent) MarshalJSON() ([]byte, error) {
	type alias BufferedEvent
	return json.Marshal(struct {
		alias
		Attrs map[string]interface{} `json:"attrs,omitempty"`
	}{
		alias: alias(b),
		Attrs: encodeAttrs(b.Attrs),
	})
}

// UnmarshalJSON 自定义解码：还原 time.Time / int64 标记。
func (b *BufferedEvent) UnmarshalJSON(data []byte) error {
	type alias BufferedEvent
	aux := struct {
		alias
		Attrs map[string]interface{} `json:"attrs,omitempty"`
	}{}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	*b = BufferedEvent(aux.alias)
	b.Attrs = decodeAttrs(aux.Attrs)
	return nil
}

// encodeAttrs 把 time.Time 编码为 {"__time__": "..."}，
// int64 编码为 {"__int__": 数值}。
func encodeAttrs(attrs map[string]interface{}) map[string]interface{} {
	if len(attrs) == 0 {
		return nil
	}
	out := make(map[string]interface{}, len(attrs))
	for k, v := range attrs {
		switch x := v.(type) {
		case time.Time:
			out[k] = map[string]string{timeMarkerKey: x.Format(time.RFC3339Nano)}
		case int64:
			out[k] = map[string]int64{intMarkerKey: x}
		default:
			out[k] = v
		}
	}
	return out
}

// decodeAttrs 还原 time.Time / int64 标记，其余值保持 JSON 默认类型。
func decodeAttrs(attrs map[string]interface{}) map[string]interface{} {
	if len(attrs) == 0 {
		return nil
	}
	out := make(map[string]interface{}, len(attrs))
	for k, v := range attrs {
		if m, ok := v.(map[string]interface{}); ok {
			if raw, isTime := m[timeMarkerKey]; isTime && len(m) == 1 {
				if s, ok := raw.(string); ok {
					if ts, err := time.Parse(time.RFC3339Nano, s); err == nil {
						out[k] = ts
						continue
					}
				}
			}
			if raw, isInt := m[intMarkerKey]; isInt && len(m) == 1 {
				if f, ok := raw.(float64); ok {
					out[k] = int64(f)
					continue
				}
			}
		}
		out[k] = v
	}
	return out
}

// DedupEntry 是去重状态的可序列化条目。
type DedupEntry struct {
	Fingerprint uint64 `json:"fingerprint"`
	Seq         uint64 `json:"seq"`
}

// stateFile 是磁盘上的序列化形式。
type stateFile struct {
	Offset       int64                 `json:"offset"`
	SinkOffset   int64                 `json:"sink_offset"`
	Watermark    time.Time             `json:"watermark"`
	EmittedFloor time.Time             `json:"emitted_floor,omitempty"`
	HasFloor     bool                  `json:"has_floor,omitempty"`
	DedupState   map[string]DedupEntry `json:"dedup_state"`
	Buffered     []BufferedEvent       `json:"buffered"`
	Committed    time.Time             `json:"committed"`
	Generation   uint64                `json:"generation"`
}

// Store 是断点的文件存储。
type Store struct {
	path string
	ttl  time.Duration
	now  func() time.Time // 可注入时钟，便于测试过期逻辑
}

// NewStore 创建断点存储；ttl 为断点有效期（<=0 表示不过期）。
func NewStore(path string, ttl time.Duration) *Store {
	return &Store{path: path, ttl: ttl, now: time.Now}
}

// SetClock 注入时钟（测试用）。
func (s *Store) SetClock(now func() time.Time) {
	s.now = now
}

// Save 原子地写入断点（先写临时文件再重命名）。
func (s *Store) Save(st State) error {
	if st.Committed.IsZero() {
		st.Committed = s.now()
	}
	f := stateFile{
		Offset:       st.Offset,
		SinkOffset:   st.SinkOffset,
		Watermark:    st.Watermark,
		EmittedFloor: st.EmittedFloor,
		HasFloor:     st.HasFloor,
		DedupState:   st.DedupState,
		Buffered:     st.Buffered,
		Committed:    st.Committed,
		Generation:   st.Generation,
	}
	data, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal checkpoint: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return fmt.Errorf("create checkpoint dir: %w", err)
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("write checkpoint tmp: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("commit checkpoint: %w", err)
	}
	return nil
}

// Load 读取并校验断点：不存在返回 nil,nil；过期返回 ErrExpired。
func (s *Store) Load() (*State, error) {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read checkpoint: %w", err)
	}
	var f stateFile
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("corrupt checkpoint: %w", err)
	}
	if s.ttl > 0 && s.now().Sub(f.Committed) > s.ttl {
		return nil, fmt.Errorf("%w: committed %s, ttl %s", ErrExpired, f.Committed, s.ttl)
	}
	return &State{
		Offset:       f.Offset,
		SinkOffset:   f.SinkOffset,
		Watermark:    f.Watermark,
		EmittedFloor: f.EmittedFloor,
		HasFloor:     f.HasFloor,
		DedupState:   f.DedupState,
		Buffered:     f.Buffered,
		Committed:    f.Committed,
		Generation:   f.Generation,
	}, nil
}

// ValidateResume 校验从 resumeOffset 恢复是否合法：
// 早于已提交位点的回退请求返回 ErrRollback。
func ValidateResume(committed, resumeOffset int64) error {
	if resumeOffset < committed {
		return fmt.Errorf("%w: resume offset %d < committed %d", ErrRollback, resumeOffset, committed)
	}
	return nil
}
