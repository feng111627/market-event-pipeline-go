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
	Offset     int64                 // 已提交的源位点（下一条待读）
	Watermark  time.Time             // 已提交的水位线基准（maxSeen）
	DedupState map[string]DedupEntry // 去重状态快照
	Buffered   []BufferedEvent       // 已处理但未输出（滞留在重排缓冲）的事件
	Committed  time.Time             // 断点提交时间
	Generation uint64                // 单调递增的代际号
}

// BufferedEvent 是重排缓冲中事件的可序列化形式。
type BufferedEvent struct {
	ID            string                 `json:"id"`
	Symbol        string                 `json:"symbol"`
	Price         float64                `json:"price"`
	Volume        float64                `json:"volume"`
	Seq           uint64                 `json:"seq"`
	EventTime     time.Time              `json:"event_time"`
	SchemaVersion int                    `json:"schema_version"`
	Attrs         map[string]interface{} `json:"attrs,omitempty"` // 已声明扩展字段，随断点完整恢复
}

// timeMarker 包裹 attrs 中的 time.Time，避免 JSON 往返后退化为字符串、
// 改变内容指纹（保证缺省与转换语义可复现）。
const timeMarker = "$time"

func encodeAttrs(in map[string]interface{}) map[string]interface{} {
	if in == nil {
		return nil
	}
	out := make(map[string]interface{}, len(in))
	for k, v := range in {
		out[k] = encodeValue(v)
	}
	return out
}

func encodeValue(v interface{}) interface{} {
	switch t := v.(type) {
	case time.Time:
		return map[string]interface{}{timeMarker: t.Format(time.RFC3339Nano)}
	case map[string]interface{}:
		return encodeAttrs(t)
	default:
		return v
	}
}

func decodeAttrs(in map[string]interface{}) (map[string]interface{}, error) {
	if in == nil {
		return nil, nil
	}
	out := make(map[string]interface{}, len(in))
	for k, v := range in {
		dv, err := decodeValue(v)
		if err != nil {
			return nil, fmt.Errorf("attr %q: %w", k, err)
		}
		out[k] = dv
	}
	return out, nil
}

func decodeValue(v interface{}) (interface{}, error) {
	switch t := v.(type) {
	case map[string]interface{}:
		if raw, ok := t[timeMarker].(string); ok && len(t) == 1 {
			ts, err := time.Parse(time.RFC3339Nano, raw)
			if err != nil {
				return nil, fmt.Errorf("parse %s: %w", timeMarker, err)
			}
			return ts, nil
		}
		return decodeAttrs(t)
	default:
		return v, nil
	}
}

// MarshalJSON 自定义序列化：attrs 中的时间用显式类型标记包裹。
func (b BufferedEvent) MarshalJSON() ([]byte, error) {
	type alias BufferedEvent // 避免递归
	return json.Marshal(alias(BufferedEvent{
		ID:            b.ID,
		Symbol:        b.Symbol,
		Price:         b.Price,
		Volume:        b.Volume,
		Seq:           b.Seq,
		EventTime:     b.EventTime,
		SchemaVersion: b.SchemaVersion,
		Attrs:         encodeAttrs(b.Attrs),
	}))
}

// UnmarshalJSON 自定义反序列化：还原 attrs 中的时间类型。
func (b *BufferedEvent) UnmarshalJSON(data []byte) error {
	type alias BufferedEvent
	var a alias
	if err := json.Unmarshal(data, &a); err != nil {
		return err
	}
	attrs, err := decodeAttrs(a.Attrs)
	if err != nil {
		return err
	}
	*b = BufferedEvent(a)
	b.Attrs = attrs
	return nil
}

// DedupEntry 是去重状态的可序列化条目。
type DedupEntry struct {
	Fingerprint uint64 `json:"fingerprint"`
	Seq         uint64 `json:"seq"`
}

// stateFile 是磁盘上的序列化形式。
type stateFile struct {
	Offset     int64                 `json:"offset"`
	Watermark  time.Time             `json:"watermark"`
	DedupState map[string]DedupEntry `json:"dedup_state"`
	Buffered   []BufferedEvent       `json:"buffered"`
	Committed  time.Time             `json:"committed"`
	Generation uint64                `json:"generation"`
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
		Offset:     st.Offset,
		Watermark:  st.Watermark,
		DedupState: st.DedupState,
		Buffered:   st.Buffered,
		Committed:  st.Committed,
		Generation: st.Generation,
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
		Offset:     f.Offset,
		Watermark:  f.Watermark,
		DedupState: f.DedupState,
		Buffered:   f.Buffered,
		Committed:  f.Committed,
		Generation: f.Generation,
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
