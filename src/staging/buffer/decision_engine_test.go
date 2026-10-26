package buffer

import (
	"context"
	"testing"

	"batchscheduler/src/group"
	"batchscheduler/src/staging"
	"batchscheduler/src/staging/evaluation"
	"batchscheduler/src/workqueue"
)

func TestCompletionDoesNotHideConfirmedFailure(t *testing.T) {
	b, view := fixture(t, workqueue.AllTogether)
	u := queuedUnit(t, view, "unit", workqueue.AllTogether, 0, 1)
	if err := b.Accept(context.Background(), u); err != nil {
		t.Fatal(err)
	}
	for _, member := range view.Members {
		member.Pod.Spec.NodeName = "node-a"
	}
	if err := b.Observe(context.Background(), staging.Observation{Group: u.Key().Group}); err != nil {
		t.Fatal(err)
	}
	r := b.records[u.Key()]
	r.Failure = &group.FailureInfo{Reason: workqueue.ReasonBindingRejected}
	r.ResponsibilitiesSettled = true
	before := r.Revision
	d := evaluation.EvaluateRecord(u.Key(), r, false)
	if d.Action != staging.ActionExecute || d.Operation != staging.OperationRecover {
		t.Fatalf("报喜阶段不能把尚未恢复的失败当成功: %+v", d)
	}
	if r.Revision != before || r.Failure.Reason != workqueue.ReasonBindingRejected {
		t.Fatal("评估修改了 Record")
	}
}

func TestApplyTransitionUsesQueueVersionAndRealTransition(t *testing.T) {
	b, view := fixture(t, workqueue.AllTogether)
	w := workqueue.New()
	w.Views = b.views
	w.Staging = b
	var unit workqueue.Unit
	w.Policy = func(_ workqueue.SelectionInput, u workqueue.Unit) (workqueue.QueueDecision, error) {
		unit = u
		return workqueue.QueueDecision{To: workqueue.QueueFail, Reason: workqueue.ReasonInsufficientResources}, nil
	}
	if err := w.Select(workqueue.SelectRequest{View: view}); err != nil {
		t.Fatal(err)
	}
	b.handlers.Transition = func(_ context.Context, request workqueue.TransitionRequest) error {
		return w.Transition(request)
	}
	key := unit.Key()
	request := workqueue.TransitionRequest{Unit: unit, From: workqueue.QueueFail, To: workqueue.QueueRetry,
		ExpectedRevision: unit.State().Revision + 1, RecoveryComplete: true}
	d := staging.Decision{Group: key.Group, Unit: &key, Action: staging.ActionTransition, Transition: &request}
	if err := b.Apply(context.Background(), d); err == nil {
		t.Fatal("过期版本不能迁移")
	}
	if _, exists := w.Fail.Get(key); !exists {
		t.Fatal("过期决策改变了 Fail")
	}
	request.ExpectedRevision = unit.State().Revision
	if err := b.Apply(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	if _, exists := w.Retry.Get(key); !exists || unit.State().Revision <= request.ExpectedRevision {
		t.Fatal("真实队列迁移或版本更新未发生")
	}
	if len(b.records) != 0 {
		t.Fatal("队列迁移不应伪造 Center 记录")
	}
}
