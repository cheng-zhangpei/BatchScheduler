package executor

import (
	"context"
	"testing"

	"batchscheduler/src/group"
	"batchscheduler/src/scheduler/steps"
	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
)

func TestReportCarriesResourceMembersAndFailure(t *testing.T) {
	accepted := workqueue.PodRef{Name: "a", UID: "a"}
	waiting := workqueue.PodRef{Name: "b", UID: "b"}
	a := &executionAttempt{Attempt: steps.Attempt{Resources: &staging.ResourceResult{
		Assumed: []workqueue.PodRef{accepted},
		Unplaced: []staging.UnplacedMember{{Pod: waiting, Failure: group.FailureInfo{
			Reason: workqueue.ReasonUnschedulable, Message: "本轮未找到位置",
		}}},
	}}}
	e := NewExecutor(nil, nil, DefaultConfig())
	var received staging.Feedback
	e.Report = func(_ context.Context, f staging.Feedback) error { received = f; return nil }
	if err := e.report(context.Background(), a, staging.FeedbackProgress, staging.PhaseAssumed, "", false); err != nil {
		t.Fatal(err)
	}
	if received.Resources == nil || received.Resources.Assumed[0] != accepted || received.Resources.Unplaced[0].Pod != waiting ||
		received.Resources.Unplaced[0].Failure.Reason != workqueue.ReasonUnschedulable || received.Kind != staging.FeedbackProgress || received.ResponsibilitiesSettled {
		t.Fatalf("报告没有完整传递成员划分和错误: %+v", received)
	}
	a.Resources.Unplaced[0].Failure.Reason = workqueue.ReasonForbidden
	if received.Resources.Unplaced[0].Failure.Reason != workqueue.ReasonUnschedulable {
		t.Fatal("报告仍引用 Attempt 的可变名单")
	}
}
