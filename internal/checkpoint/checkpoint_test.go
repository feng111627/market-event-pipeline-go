package checkpoint

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestFileStore_RoundTripAndAtomicPresence(t *testing.T) {
	t.Log("【业务覆盖】断点持久化：保存后可加载；写入采用临时文件+rename，不产生半截文件")
	dir := t.TempDir()
	path := filepath.Join(dir, "cp.json")
	st := NewFileStore(path)

	got, err := st.Load(context.Background())
	if !errors.Is(err, ErrNoCheckpoint) {
		t.Fatalf("fresh load want ErrNoCheckpoint, got %v", err)
	}
	t.Log("  ✔ 无断点文件时返回 ErrNoCheckpoint（应从头启动）")

	want := State{Version: 1, Epoch: 7, InputOffset: 123, WatermarkUnix: 456, SinkSeq: 9, UpdatedAt: time.Now()}
	if err := st.Save(context.Background(), want); err != nil {
		t.Fatal(err)
	}
	got, err = st.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.Epoch != 7 || got.InputOffset != 123 || got.WatermarkUnix != 456 || got.SinkSeq != 9 {
		t.Fatalf("round trip mismatch: %+v", got)
	}
	t.Logf("  ✔ 保存/加载一致：offset=%d sink_seq=%d epoch=%d", got.InputOffset, got.SinkSeq, got.Epoch)
}

func TestValidator_AcceptsFreshCompatible(t *testing.T) {
	t.Log("【业务覆盖】恢复校验：同纪元、位置不回退、未过期的断点允许恢复")
	v := NewValidator(time.Hour)
	cp := &State{Version: 1, Epoch: 3, InputOffset: 100, WatermarkUnix: 100, SinkSeq: 5, UpdatedAt: time.Now()}
	if err := v.Validate(cp, 3, 5, time.Now()); err != nil {
		t.Fatalf("valid checkpoint rejected: %v", err)
	}
	t.Log("  ✔ epoch 相同、sink_seq=已确认 5 -> 允许恢复")
}

func TestValidator_RejectsStaleEpoch(t *testing.T) {
	t.Log("【业务覆盖】断点过期：纪元不一致（跨代）必须拒绝，不能从中间接着算")
	v := NewValidator(0)
	cp := &State{Version: 1, Epoch: 1, SinkSeq: 0, UpdatedAt: time.Now()}
	err := v.Validate(cp, 2, 0, time.Now())
	if !errors.Is(err, ErrStale) {
		t.Fatalf("want ErrStale, got %v", err)
	}
	t.Logf("  ✔ %v", err)
}

func TestValidator_RejectsExpiredByTTL(t *testing.T) {
	t.Log("【业务覆盖】断点过期：超过 TTL 必须拒绝")
	v := NewValidator(time.Minute)
	cp := &State{Version: 1, Epoch: 1, SinkSeq: 0, UpdatedAt: time.Now().Add(-2 * time.Minute)}
	err := v.Validate(cp, 1, 0, time.Now())
	if !errors.Is(err, ErrStale) {
		t.Fatalf("want ErrStale, got %v", err)
	}
	t.Logf("  ✔ %v", err)
}

func TestValidator_RejectsRegression(t *testing.T) {
	t.Log("【业务覆盖】断点回退：checkpoint 的输出位置落后于 sink 已确认位置 -> 拒绝")
	v := NewValidator(0)
	cp := &State{Version: 1, Epoch: 1, SinkSeq: 3, UpdatedAt: time.Now()}
	err := v.Validate(cp, 1, 8, time.Now())
	if !errors.Is(err, ErrRegressed) {
		t.Fatalf("want ErrRegressed, got %v", err)
	}
	t.Logf("  ✔ %v", err)
}

func TestValidator_RejectsBadVersion(t *testing.T) {
	t.Log("【业务覆盖】断点格式版本不支持 -> 拒绝")
	v := NewValidator(0)
	cp := &State{Version: 99, Epoch: 1, UpdatedAt: time.Now()}
	if err := v.Validate(cp, 1, 0, time.Now()); !errors.Is(err, ErrBadVersion) {
		t.Fatalf("want ErrBadVersion, got %v", err)
	}
	t.Log("  ✔ version=99 被拒绝")
}
