package buffer

import (
	"context"
	"testing"

	"batchscheduler/src/group"
	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
)

func TestPartialBindingReclaimReturnsOnlyReleasedMembers(t *testing.T) {
	b, view, request, f := unknownBindingReport(t, workqueue.AllowPartial)
	ref := request.Members[1]
	view.Members[0].Pod.Spec.NodeName = "node-a"
	f.Kind, f.Phase, f.Failure, f.ResponsibilitiesSettled = staging.FeedbackFinished, staging.PhaseBound, nil, true
	f.ReleasedBindings = []workqueue.PodRef{ref}
	if err := b.Report(context.Background(), f); err != nil {
		t.Fatal(err)
	}
	r := b.records[request.Unit]
	if r.Failure != nil || r.PendingAction != "" || len(r.Resources.Assumed) != 1 || len(r.Resources.Unplaced) != 1 {
		t.Fatalf("Partial 成员结果没有拆开: %+v", r)
	}
	if r.Members[request.Members[0]].Phase != group.PodBound || r.Members[ref].Phase != group.PodUnscheduled {
		t.Fatal("释放失败成员时改动了成功成员")
	}
	d := decisionFor(t, b, request.Unit)
	if d.Action != staging.ActionReturnMembers || len(d.Members) != 1 || d.Members[0] != ref || d.Reason != workqueue.ReasonBindingConfirmationExpired {
		t.Fatalf("没有只回送已释放成员: %+v", d)
	}
}

func TestPartialAllBindingsReleasedReturnsMembersThenCleansEmptyAttempt(t *testing.T) {
	b, _, request, f := unknownBindingReport(t, workqueue.AllowPartial)
	f.Kind, f.Phase, f.ResponsibilitiesSettled = staging.FeedbackFinished, staging.PhaseRecovered, true
	f.Failure = &group.FailureInfo{Reason: workqueue.ReasonBindingConfirmationExpired}
	f.ReleasedBindings = request.Members
	if err := b.Report(context.Background(), f); err != nil {
		t.Fatal(err)
	}
	b.handlers.Handoff = func(context.Context, staging.Handoff) error { return nil }
	d := decisionFor(t, b, request.Unit)
	if d.Action != staging.ActionReturnMembers || len(d.Members) != 2 {
		t.Fatalf("全部释放的 Partial 没有走部分回送: %+v", d)
	}
	if err := b.Apply(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	d = decisionFor(t, b, request.Unit)
	if d.Action != staging.ActionRelease || d.Reason != "NoRemainingMembers" {
		t.Fatalf("旧轮次没有清理，或被当成调度成功: %+v", d)
	}
	if err := b.Apply(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	groupContext, err := b.GroupContext(context.Background(), request.Unit.Group)
	if err != nil || len(groupContext.InFlight) != 0 || len(groupContext.Succeeded) != 0 {
		t.Fatalf("空轮次被错误归档: %+v, %v", groupContext, err)
	}
}

func TestBindingReleaseNotifiesResourceWaitersAfterSavingReport(t *testing.T) {
	b, _, request, f := unknownBindingReport(t, workqueue.AllTogether)
	notifications := 0
	b.handlers.WaitingGroups = func() []group.Key {
		notifications++
		if !b.mu.TryRLock() {
			t.Error("仍占着 Buffer 写锁就去通知外部队列")
			return nil
		}
		defer b.mu.RUnlock()
		if b.records[request.Unit].PendingAction != "" {
			t.Error("通知资源变化之前没有保存本轮恢复结果")
		}
		return nil
	}
	b.handlers.QueueSnapshot = func(group.Key) []workqueue.QueuedUnit { return nil }
	f.Kind, f.Phase, f.ResponsibilitiesSettled = staging.FeedbackFinished, staging.PhaseRecovered, true
	f.Failure = &group.FailureInfo{Reason: workqueue.ReasonBindingConfirmationExpired}
	f.ReleasedBindings = request.Members
	if err := b.Report(context.Background(), f); err != nil {
		t.Fatal(err)
	}
	if notifications != 1 {
		t.Fatal("释放暂占后没有通知等待资源变化的队列")
	}
}

func TestAtomicAllBindingsReleasedUsesWholeReturn(t *testing.T) {
	b, _, request, f := unknownBindingReport(t, workqueue.AllTogether)
	f.Kind, f.Phase, f.ResponsibilitiesSettled = staging.FeedbackFinished, staging.PhaseRecovered, true
	f.Failure = &group.FailureInfo{Reason: workqueue.ReasonBindingConfirmationExpired}
	f.ReleasedBindings = request.Members
	if err := b.Report(context.Background(), f); err != nil {
		t.Fatal(err)
	}
	d := decisionFor(t, b, request.Unit)
	if d.Action != staging.ActionReturn || d.To != workqueue.QueueFail || len(d.Members) != 2 {
		t.Fatalf("Atomic 没有整组回送: %+v", d)
	}
}

func unknownBindingReport(t *testing.T, mode workqueue.SubmitMode) (*Buffer, *group.View, staging.Execution, staging.Feedback) {
	t.Helper()
	b, view := fixture(t, mode)
	u := queuedUnit(t, view, "unit", mode, 0, 1)
	var request staging.Execution
	b.handlers.Execute = func(_ context.Context, r staging.Execution) error { request = r; return nil }
	if err := b.Accept(context.Background(), u); err != nil {
		t.Fatal(err)
	}
	if err := b.Apply(context.Background(), decisionFor(t, b, u.Key())); err != nil {
		t.Fatal(err)
	}
	plan := make(map[workqueue.PodRef]string)
	for _, ref := range request.Members {
		plan[ref] = "node-a"
	}
	f := staging.Feedback{Unit: request.Unit, ActionID: request.ActionID, Operation: staging.OperationRun,
		Kind: staging.FeedbackUnknown, Phase: staging.PhaseRecovering, Plan: plan,
		Failure: &group.FailureInfo{Reason: workqueue.ReasonBindingUnknown}}
	if err := b.Report(context.Background(), f); err != nil {
		t.Fatal(err)
	}
	return b, view, request, f
}
