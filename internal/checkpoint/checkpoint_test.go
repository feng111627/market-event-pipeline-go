package checkpoint

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestSaveLoadRoundtrip 验证断点写入后可完整读回。
func TestSaveLoadRoundtrip(t *testing.T) {
	t.Log("业务场景: 正常运行中周期性提交断点，重启后完整读回")
	path := filepath.Join(t.TempDir(), "cp.json")
	s := NewStore(path, time.Hour)
	want := State{
		Offset:    42,
		Watermark: time.Date(2026, 10, 6, 10, 0, 0, 0, time.UTC),
		DedupState: map[string]DedupEntry{
			"e1": {Fingerprint: 123, Seq: 1},
		},
		Generation: 7,
	}
	if err := s.Save(want); err != nil {
		t.Fatalf("Save 失败: %v", err)
	}
	got, err := s.Load()
	if err != nil {
		t.Fatalf("Load 失败: %v", err)
	}
	if got.Offset != 42 || got.Generation != 7 || !got.Watermark.Equal(want.Watermark) ||
		got.DedupState["e1"].Fingerprint != 123 {
		t.Fatalf("读回内容不一致: %+v", got)
	}
	t.Logf("用例结果: 断点完整读回 offset=%d generation=%d [通过]", got.Offset, got.Generation)
}

// TestLoadNotExist 验证无断点时返回 nil 而非报错（首次启动场景）。
func TestLoadNotExist(t *testing.T) {
	t.Log("业务场景: 首次启动无断点文件，应从头开始而不是报错")
	s := NewStore(filepath.Join(t.TempDir(), "cp.json"), time.Hour)
	st, err := s.Load()
	if err != nil || st != nil {
		t.Fatalf("无断点应返回 nil,nil, 得到 %v, %v", st, err)
	}
	t.Log("用例结果: 无断点返回 nil,nil [通过]")
}

// TestExpired 验证过期断点被明确拒绝。
func TestExpired(t *testing.T) {
	t.Log("业务场景: 断点超过有效期，恢复必须被拒绝而不是从过旧状态接着算")
	path := filepath.Join(t.TempDir(), "cp.json")
	now := time.Now()
	s := NewStore(path, time.Hour)
	s.SetClock(func() time.Time { return now })
	if err := s.Save(State{Offset: 10}); err != nil {
		t.Fatalf("Save 失败: %v", err)
	}
	s.SetClock(func() time.Time { return now.Add(2 * time.Hour) })
	_, err := s.Load()
	if !errors.Is(err, ErrExpired) {
		t.Fatalf("应返回 ErrExpired, 得到 %v", err)
	}
	t.Logf("用例结果: 过期断点被拒绝 err=%v [通过]", err)
}

// TestRollbackRejected 验证回退到已提交位点之前被明确拒绝。
func TestRollbackRejected(t *testing.T) {
	t.Log("业务场景: 请求从比已提交位点更早的位置恢复，属回退，必须拒绝")
	if err := ValidateResume(100, 50); !errors.Is(err, ErrRollback) {
		t.Fatalf("应返回 ErrRollback, 得到 %v", err)
	}
	if err := ValidateResume(100, 100); err != nil {
		t.Fatalf("相同位点应允许, 得到 %v", err)
	}
	t.Log("用例结果: 回退被拒绝，相同位点允许 [通过]")
}

// TestAtomicWrite 验证崩溃不会留下半截断点文件（tmp 文件不影响正式文件）。
func TestAtomicWrite(t *testing.T) {
	t.Log("业务场景: 写入过程中崩溃，正式断点文件保持上一次完整内容")
	path := filepath.Join(t.TempDir(), "cp.json")
	s := NewStore(path, time.Hour)
	if err := s.Save(State{Offset: 1, Generation: 1}); err != nil {
		t.Fatalf("Save 失败: %v", err)
	}
	// 模拟崩溃留下的半截临时文件。
	if err := os.WriteFile(path+".tmp", []byte(`{"offset": 999, broken`), 0o644); err != nil {
		t.Fatal(err)
	}
	st, err := s.Load()
	if err != nil || st.Offset != 1 {
		t.Fatalf("半截 tmp 文件不应影响正式断点: %v, %+v", err, st)
	}
	t.Log("用例结果: 半截临时文件被忽略，正式断点完好 [通过]")
}
