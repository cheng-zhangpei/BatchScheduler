package buffer

import (
	"context"
	"errors"
	"testing"

	"batchscheduler/src/staging"
)

func TestApplyReturnMembersFailureKeepsExecutionAndOwnership(t *testing.T) {
	b, feedback := startPartialResourceReport(t)
	if err := b.Report(context.Background(), feedback); err != nil {
		t.Fatal(err)
	}
	want := errors.New("队列尚未接住任务")
	b.handlers.Handoff = func(context.Context, staging.Handoff) error { return want }
	d := decisionFor(t, b, feedback.Unit)
	if err := b.Apply(context.Background(), d); !errors.Is(err, want) {
		t.Fatalf("没有带出交接失败: %v", err)
	}
	r := b.records[feedback.Unit]
	if r == nil || r.PendingAction != feedback.ActionID || r.ReturningMembers || len(r.ReturnedMembers) != 0 || len(b.owners[feedback.Unit.Group]) != 2 {
		t.Fatal("交接失败却清除了绑定 Pending、记录或成员归属")
	}
	// 失败后仍能正常接收原执行反馈，后续也可以重新提出交接。
	feedback.Phase = staging.PhaseBinding
	if err := b.Report(context.Background(), feedback); err != nil {
		t.Fatal(err)
	}
	if next := decisionFor(t, b, feedback.Unit); next.Action != staging.ActionReturnMembers {
		t.Fatalf("交接失败后没有保留可再次回送的成员: %+v", next)
	}
}
