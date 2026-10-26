package buffer

import (
	"context"
	"testing"
	"time"

	"batchscheduler/src/group"
	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
)

func queuedFailure(t *testing.T, reason string, recovered bool) (*Buffer, *workqueue.WorkQueue, workqueue.Unit) {
	t.Helper()
	b, view := fixture(t, workqueue.AllTogether)
	w := workqueue.New()
	w.Views, w.Staging = b.views, b
	var unit workqueue.Unit
	w.Policy = func(_ workqueue.SelectionInput, u workqueue.Unit) (workqueue.QueueDecision, error) {
		unit = u
		return workqueue.QueueDecision{To: workqueue.QueueFail, Reason: reason, RecoveryComplete: recovered}, nil
	}
	if err := w.Select(workqueue.SelectRequest{View: view}); err != nil {
		t.Fatal(err)
	}
	b.handlers.QueueSnapshot, b.handlers.WaitingGroups = w.GroupSnapshot, w.WaitingGroups
	b.handlers.Transition = func(_ context.Context, request workqueue.TransitionRequest) error { return w.Transition(request) }
	b.handlers.SelectGroup = func(_ context.Context, view *group.View) error { return w.Select(workqueue.SelectRequest{View: view}) }
	return b, w, unit
}

// 不手写 Decision：Observe/时钟通知 → Evaluate → Apply → 真实 WorkQueue.Transition。
func TestAutomaticQueueFlowFromEvaluation(t *testing.T) {
	for _, reason := range []string{workqueue.ReasonInsufficientResources, workqueue.ReasonUnschedulable, workqueue.ReasonTransientSchedulingError} {
		t.Run(reason, func(t *testing.T) {
			b, w, unit := queuedFailure(t, reason, true)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			errors := make(chan error, 10)
			done := make(chan error, 1)
			checksDone := make(chan struct{})
			go func() { defer close(checksDone); b.RunQueueChecks(ctx, 5*time.Millisecond) }()
			go func() {
				done <- b.RunEvaluations(ctx, 8, func(ctx context.Context, ds []staging.Decision) error {
					for _, d := range ds {
						if err := b.Apply(ctx, d); err != nil {
							errors <- err
							return err
						}
					}
					return nil
				})
			}()
			defer func() {
				cancel()
				<-checksDone
				if err := <-done; err != nil {
					t.Error(err)
				}
			}()
			if err := b.Observe(ctx, staging.Observation{Group: unit.Key().Group}); err != nil {
				t.Fatal(err)
			}
			waitForQueue(t, ctx, unit, workqueue.QueueRetry)
			if reason == workqueue.ReasonInsufficientResources || reason == workqueue.ReasonUnschedulable {
				b.NotifyRetry(workqueue.RetryOnDependency) // 不相关事件不得唤醒资源/通用调度等待。
				ds, err := b.Evaluate(ctx, unit.Key().Group)
				if err != nil {
					t.Fatal(err)
				}
				for _, d := range ds {
					if d.Action == staging.ActionTransition {
						t.Fatal("不相关事件产生了激活决策")
					}
				}
				b.NotifyRetry(workqueue.RetryOnResources)
			}
			got, err := w.Active.Pop(ctx)
			if err != nil || got.Key() != unit.Key() {
				t.Fatalf("自动恢复未到 Active: %v", err)
			}
			if _, exists := w.Retry.Get(unit.Key()); exists {
				t.Fatal("迁移后 Retry 未清理")
			}
			select {
			case err := <-errors:
				t.Fatal(err)
			default:
			}
		})
	}
}

func waitForQueue(t *testing.T, ctx context.Context, unit workqueue.Unit, queue workqueue.QueueName) {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for unit.State().Queue != queue {
		select {
		case <-ctx.Done():
			t.Fatalf("等待 %s 超时，当前 %s", queue, unit.State().Queue)
		case <-ticker.C:
		}
	}
}

func TestQueueEvaluationDoesNotRetryUnsettledOrConfigurationFailure(t *testing.T) {
	for _, tc := range []struct {
		reason    string
		recovered bool
	}{
		{workqueue.ReasonInsufficientResources, false},
		{workqueue.ReasonForbidden, true},
		{workqueue.ReasonInvalidSpec, true},
		{workqueue.ReasonBindingUnknown, true},
		{workqueue.ReasonSchedulingError, true},
	} {
		t.Run(tc.reason, func(t *testing.T) {
			b, _, unit := queuedFailure(t, tc.reason, tc.recovered)
			ds, err := b.Evaluate(context.Background(), unit.Key().Group)
			if err != nil {
				t.Fatal(err)
			}
			for _, d := range ds {
				if d.Action == staging.ActionTransition {
					t.Fatalf("错误获得自动重试: %+v", d)
				}
			}
		})
	}
}
