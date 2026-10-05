package sink

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func emit(seq int64, id string) Outcome {
	return Outcome{Seq: seq, Reason: "accepted", SeqID: id, Detail: "x"}
}

func TestFileSink_SequentialAckAndReopenDedup(t *testing.T) {
	t.Log("【业务覆盖】结果端幂等：只接受 ack+1；重开文件后从 sidecar 恢复确认位置")
	dir := t.TempDir()
	path := filepath.Join(dir, "out.jsonl")

	s, err := OpenFileSink(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := s.Emit(ctx, emit(1, "a")); err != nil {
		t.Fatal(err)
	}
	if err := s.Emit(ctx, emit(2, "b")); err != nil {
		t.Fatal(err)
	}
	if err := s.Emit(ctx, emit(1, "a-again")); !errors.Is(err, ErrAlreadyAcked) {
		t.Fatalf("re-emit seq=1 want ErrAlreadyAcked, got %v", err)
	}
	t.Log("  ✔ 重发 seq=1 被拒绝（ErrAlreadyAcked），不会重复计入")
	if err := s.Emit(ctx, emit(3, "hole")); err != nil {
		t.Fatal(err)
	}
	if err := s.Emit(ctx, emit(5, "jump")); !errors.Is(err, ErrOutOfOrderSeq) {
		t.Fatalf("seq hole want ErrOutOfOrderSeq, got %v", err)
	}
	t.Log("  ✔ 序号跳跃（3->5）被拒绝，保证序号连续")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := OpenFileSink(path)
	if err != nil {
		t.Fatal(err)
	}
	if s2.AcknowledgedSeq() != 3 {
		t.Fatalf("reopened ack want 3, got %d", s2.AcknowledgedSeq())
	}
	if err := s2.Emit(ctx, emit(4, "c")); err != nil {
		t.Fatalf("resume emit seq=4: %v", err)
	}
	t.Log("  ✔ 重开后 ack=3，续写 seq=4 成功")
	s2.Close()
}

func TestFileSink_TruncatesUnackedTailOnReopen(t *testing.T) {
	t.Log("【业务覆盖】崩溃恢复：输出文件中超过 sidecar 确认位置的尾部必须被截断（模拟已写未提交）")
	dir := t.TempDir()
	path := filepath.Join(dir, "out.jsonl")
	side := path + ".ack"

	// 造场景：5 行数据但 sidecar 只确认到 3（崩溃发生在写入第 4、5 行后、提交前）
	full := ""
	for i := 1; i <= 5; i++ {
		full += `{"seq":` + itoaS(i) + `,"reason":"accepted","detail":"x"}` + "\n"
	}
	if err := os.WriteFile(path, []byte(full), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(side, []byte("3"), 0o644); err != nil {
		t.Fatal(err)
	}

	s, err := OpenFileSink(path)
	if err != nil {
		t.Fatal(err)
	}
	if s.AcknowledgedSeq() != 3 {
		t.Fatalf("ack want 3, got %d", s.AcknowledgedSeq())
	}
	if err := s.Emit(context.Background(), emit(4, "rerun-4")); err != nil {
		t.Fatalf("re-emit 4 after tail truncation: %v", err)
	}
	s.Close()

	b, _ := os.ReadFile(path)
	if n := lines(b); n != 4 {
		t.Fatalf("after recovery want 4 lines, got %d:\n%s", n, b)
	}
	if !contains(string(b), "rerun-4") {
		t.Fatalf("line 4 should be the re-emitted record")
	}
	t.Log("  ✔ 未确认的第 4、5 行被截断，重放第 4 行成功，最终恰好 4 行（不多不少）")
}

func TestMemorySink_IdempotentContract(t *testing.T) {
	t.Log("【业务覆盖】内存 sink 与文件 sink 遵守相同序号幂等契约（测试基建一致性）")
	var m MemorySink
	ctx := context.Background()
	if err := m.Emit(ctx, emit(1, "a")); err != nil {
		t.Fatal(err)
	}
	if err := m.Emit(ctx, emit(1, "a")); !errors.Is(err, ErrAlreadyAcked) {
		t.Fatalf("want ErrAlreadyAcked, got %v", err)
	}
	if err := m.Emit(ctx, emit(3, "c")); !errors.Is(err, ErrOutOfOrderSeq) {
		t.Fatalf("want ErrOutOfOrderSeq, got %v", err)
	}
	if len(m.Items()) != 1 || m.AcknowledgedSeq() != 1 {
		t.Fatalf("memory sink state wrong: %+v", m.Items())
	}
	t.Log("  ✔ 重复拒绝、空洞拒绝、仅 1 条留存")
}

func lines(b []byte) int {
	n := 0
	for _, c := range b {
		if c == '\n' {
			n++
		}
	}
	return n
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func itoaS(n int) string {
	if n == 0 {
		return "0"
	}
	var b [4]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
