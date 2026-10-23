package buffer

import (
	"context"
	"testing"
	"time"

	"batchscheduler/src/group"
	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
	corev1 "k8s.io/api/core/v1"
)

// 执行器仍然模拟，但 Handoff、Select、Fail → Retry → Active 都使用真实实现。
func TestReturnHandoffWithRealSelectAndTransitions(t *testing.T) {
	for _, mode := range []workqueue.SubmitMode{workqueue.AllTogether, workqueue.AllowPartial} {
		t.Run(string(mode), func(t *testing.T) {
			b, view := fixture(mode)
			w := workqueue.New()
			w.Views, w.Staging = b.views, b
			b.handlers.QueueSnapshot, b.handlers.WaitingGroups = w.GroupSnapshot, w.WaitingGroups
			b.handlers.Transition = func(_ context.Context, request workqueue.TransitionRequest) error { return w.Transition(request) }
			b.handlers.Execute = simulatedExecutor(b, view, true)
			b.handlers.Handoff = func(_ context.Context, request staging.Handoff) error {
				if request.Decision.Action != staging.ActionReturn || request.Decision.To != workqueue.QueueFail {
					t.Fatal("应当失败回送")
				}
				if !b.ContainsPod(request.Unit.Key().Group, request.Unit.Members()[0].UID) {
					t.Fatal("回送前 Buffer 提前删记录")
				}
				return w.Select(workqueue.SelectRequest{Return: &workqueue.ReturnRequest{
					Previous: request.Unit.Key(), Members: request.Decision.Members,
					Failure: request.Decision.Failure, RecoveryComplete: true,
				}})
			}
			if err := w.Select(workqueue.SelectRequest{View: view}); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			old, err := w.Active.Pop(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := b.Accept(ctx, old); err != nil {
				t.Fatal(err)
			}
			for _, action := range []staging.Action{staging.ActionExecute, staging.ActionReturn} {
				d := decisionFor(t, b, old.Key())
				if d.Action != action {
					t.Fatalf("错误阶段: %+v", d)
				}
				if err := b.Apply(ctx, d); err != nil {
					t.Fatal(err)
				}
			}
			if len(b.records) != 0 || len(b.groups) != 0 || len(b.owners) != 0 || old.State().Phase != workqueue.UnitDone {
				t.Fatal("回送后旧轮次没有结束")
			}
			queued := w.GroupSnapshot(old.Key().Group)
			if len(queued) != 1 || queued[0].State.Queue != workqueue.QueueFail || len(queued[0].Unit.Members()) != 2 || !queued[0].Failure.RecoveryComplete {
				t.Fatalf("Select 没有接住整批成员: %+v", queued)
			}
			next := queued[0].Unit
			// 不手写迁移决定，让 Evaluate 按回送错误自动安排 Fail → Retry。
			d := decisionFor(t, b, next.Key())
			if d.Action != staging.ActionTransition || d.Transition.From != workqueue.QueueFail || d.Transition.To != workqueue.QueueRetry {
				t.Fatalf("没有按错误生成迁移: %+v", d)
			}
			if err := b.Apply(ctx, d); err != nil {
				t.Fatal(err)
			}
			// 与 CPU 不足相关的资源变化，才让这个任务重新进入 Active。
			b.NotifyRetry(workqueue.RetryOnResources)
			d = decisionFor(t, b, next.Key())
			if d.Action != staging.ActionTransition || d.Transition.To != workqueue.QueueActive {
				t.Fatalf("没有生成激活决定: %+v", d)
			}
			if err := b.Apply(ctx, d); err != nil {
				t.Fatal(err)
			}
			active, err := w.Active.Pop(ctx)
			if err != nil || active != next {
				t.Fatalf("没有取到下一轮任务: %v", err)
			}
			if err := b.Accept(ctx, active); err != nil {
				t.Fatal(err)
			}
			if d := decisionFor(t, b, next.Key()); d.Operation != staging.OperationRun {
				t.Fatalf("旧失败错误被当成新一轮执行结果: %+v", d)
			}
		})
	}
}

func TestPartialReturnRediscoverUnboundHealthyMembers(t *testing.T) {
	b, view := fixture(workqueue.AllowPartial)
	w := workqueue.New()
	w.Views, w.Staging = b.views, b
	b.handlers.Execute = simulatedExecutor(b, view, false)
	b.handlers.SelectGroup = func(_ context.Context, view *group.View) error {
		return w.Select(workqueue.SelectRequest{View: view})
	}
	b.handlers.Handoff = func(_ context.Context, request staging.Handoff) error {
		return w.Select(workqueue.SelectRequest{Return: &workqueue.ReturnRequest{
			Previous: request.Unit.Key(), Members: request.Decision.Members,
			Failure: request.Decision.Failure, RecoveryComplete: true,
		}})
	}
	if err := w.Select(workqueue.SelectRequest{View: view}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	old, err := w.Active.Pop(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Accept(ctx, old); err != nil {
		t.Fatal(err)
	}
	// 两个 Pod 都未绑定，只有第一个观察到调度失败。
	view.Members[0].Pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodScheduled,
		Status: corev1.ConditionFalse, Reason: corev1.PodReasonUnschedulable}}
	d := decisionFor(t, b, old.Key())
	if d.Operation != staging.OperationRecover {
		t.Fatalf("应该先恢复: %+v", d)
	}
	if err := b.Apply(ctx, d); err != nil {
		t.Fatal(err)
	}
	d = decisionFor(t, b, old.Key())
	if d.Action != staging.ActionReturn || len(d.Members) != 1 {
		t.Fatalf("只应回送失败成员: %+v", d)
	}
	for b.evaluations.Len() > 0 {
		key, _ := b.evaluations.Get()
		b.evaluations.Done(key)
	}
	if err := b.Apply(ctx, d); err != nil {
		t.Fatal(err)
	}
	queued := w.GroupSnapshot(old.Key().Group)
	if len(queued) != 1 || len(queued[0].Unit.Members()) != 1 || queued[0].Unit.Members()[0].UID != view.Members[0].Pod.UID {
		t.Fatal("失败成员没有单独进入 Fail")
	}
	if b.evaluations.Len() == 0 {
		t.Fatal("没有通知继续检查剩余成员")
	}
	// 清理之后，Evaluate 发现未绑定且不再被 Buffer 持有的成员，交给普通 Select。
	decisions, err := b.Evaluate(ctx, old.Key().Group)
	if err != nil {
		t.Fatal(err)
	}
	selected := false
	for _, decision := range decisions {
		if decision.Action == staging.ActionSelect {
			if err := b.Apply(ctx, decision); err != nil {
				t.Fatal(err)
			}
			selected = true
		}
	}
	if !selected {
		t.Fatal("没有生成普通 Select 决定")
	}
	healthy, err := w.Active.Pop(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(healthy.Members()) != 1 || healthy.Members()[0].UID != view.Members[1].Pod.UID || healthy.State().LastFailure != nil {
		t.Fatal("正常成员没有独立进入 Active")
	}
}
