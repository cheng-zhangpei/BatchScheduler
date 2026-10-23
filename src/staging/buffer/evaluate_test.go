package buffer

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"batchscheduler/src/group"
	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
	corev1 "k8s.io/api/core/v1"
)

func TestEvaluateAttemptDecisions(t *testing.T) {
	for _, tc := range []struct {
		name   string
		edit   func(*staging.Record)
		action staging.Action
		op     staging.Operation
	}{
		{"ready", func(*staging.Record) {}, staging.ActionExecute, staging.OperationRun},
		{"no-observation", func(r *staging.Record) { r.Observed = nil }, staging.ActionWait, ""},
		{"operation-in-flight", func(r *staging.Record) {
			r.PendingAction = "binding-1"
			r.Failure = &group.FailureInfo{Reason: workqueue.ReasonBindingRejected}
		}, staging.ActionWait, ""},
		{"binding-unknown", func(r *staging.Record) {
			for ref, s := range r.Members {
				s.Phase = group.PodBindingUnknown
				r.Members[ref] = s
			}
		}, staging.ActionWait, ""},
		{"failure-needs-recovery", func(r *staging.Record) { r.Failure = &group.FailureInfo{Reason: workqueue.ReasonInsufficientResources} }, staging.ActionExecute, staging.OperationRecover},
		{"recovery-confirmed", func(r *staging.Record) {
			r.Failure = &group.FailureInfo{Reason: workqueue.ReasonInsufficientResources}
			r.RecoveryComplete = true
			r.ResponsibilitiesSettled = true
		}, staging.ActionReturn, ""},
		{"bound-unsettled", func(r *staging.Record) {
			for ref, f := range r.Observed.Members {
				f.NodeName = "node-a"
				r.Observed.Members[ref] = f
			}
		}, staging.ActionWait, ""},
		{"bound-settled", func(r *staging.Record) {
			for ref, f := range r.Observed.Members {
				f.NodeName = "node-a"
				r.Observed.Members[ref] = f
			}
			r.ResponsibilitiesSettled = true
		}, staging.ActionRelease, ""},
		{"missing-member", func(r *staging.Record) {
			for ref, f := range r.Observed.Members {
				f.Present = false
				r.Observed.Members[ref] = f
			}
		}, staging.ActionExecute, staging.OperationRecover},
		{"group-deleted-unsettled", func(r *staging.Record) { r.Observed.Exists = false }, staging.ActionExecute, staging.OperationRecover},
		{"group-deleted-settled", func(r *staging.Record) { r.Observed.Exists = false; r.ResponsibilitiesSettled = true }, staging.ActionRelease, ""},
		{"reserved-needs-feedback", func(r *staging.Record) {
			for ref, s := range r.Members {
				s.Phase = group.PodReserved
				r.Members[ref] = s
			}
		}, staging.ActionWait, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, view := fixture(workqueue.AllTogether)
			u := queuedUnit(t, view, "unit", workqueue.AllTogether, 0, 1)
			if err := b.Accept(context.Background(), u); err != nil {
				t.Fatal(err)
			}
			if err := b.Observe(context.Background(), staging.Observation{Group: u.Key().Group}); err != nil {
				t.Fatal(err)
			}
			r := b.records[u.Key()]
			tc.edit(r)
			decisions, err := b.Evaluate(context.Background(), u.Key().Group)
			if err != nil || len(decisions) != 1 {
				t.Fatalf("decisions=%+v err=%v", decisions, err)
			}
			d := decisions[0]
			if d.Action != tc.action || d.Operation != tc.op || d.Revision != r.Revision {
				t.Fatalf("unexpected decision: %+v", d)
			}
			if tc.name == "recovery-confirmed" && d.To != workqueue.QueueFail {
				t.Fatal("恢复后应建议 Fail，不能擅自升级 FastRetry")
			}
			if len(b.records) != 1 || !b.ContainsPod(u.Key().Group, u.Members()[0].UID) {
				t.Fatal("Evaluate 不应实际清理或迁移")
			}
		})
	}
}

func TestEvaluateFailureScopeAndIndependentPartialProgress(t *testing.T) {
	for _, mode := range []workqueue.SubmitMode{workqueue.AllTogether, workqueue.AllowPartial} {
		t.Run(string(mode), func(t *testing.T) {
			b, view := fixture(mode)
			u := queuedUnit(t, view, "first", mode, 0, 1)
			if err := b.Accept(context.Background(), u); err != nil {
				t.Fatal(err)
			}
			if err := b.Observe(context.Background(), staging.Observation{Group: u.Key().Group}); err != nil {
				t.Fatal(err)
			}
			r := b.records[u.Key()]
			ref := u.Members()[0]
			s := r.Members[ref]
			s.LastFailure = &group.FailureInfo{Reason: workqueue.ReasonInsufficientResources}
			r.Members[ref] = s
			ds, err := b.Evaluate(context.Background(), u.Key().Group)
			if err != nil {
				t.Fatal(err)
			}
			want := 1
			if mode == workqueue.AllTogether {
				want = 2
			}
			if ds[0].Operation != staging.OperationRecover || len(ds[0].Members) != want {
				t.Fatalf("错误的失败范围: %+v", ds)
			}
			// 结果不能持有内部 Failure 指针。
			ds[0].Failure.Reason = "mutated"
			if r.Members[ref].LastFailure.Reason != workqueue.ReasonInsufficientResources {
				t.Fatal("决策共享了内部可变错误")
			}
		})
	}
	b, view := fixture(workqueue.AllowPartial)
	first := queuedUnit(t, view, "first", workqueue.AllowPartial, 0)
	second := queuedUnit(t, view, "second", workqueue.AllowPartial, 1)
	for _, u := range []workqueue.Unit{first, second} {
		if err := b.Accept(context.Background(), u); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Observe(context.Background(), staging.Observation{Group: first.Key().Group}); err != nil {
		t.Fatal(err)
	}
	b.records[first.Key()].PendingAction = "old-binding"
	ds, err := b.Evaluate(context.Background(), first.Key().Group)
	if err != nil || len(ds) != 2 || ds[0].Action != staging.ActionWait || ds[1].Operation != staging.OperationRun {
		t.Fatalf("Partial 单元不应被同组其他在途单元阻塞: %+v %v", ds, err)
	}
}

func TestEvaluateUnheldGroupAndChangedDeclaration(t *testing.T) {
	b, view := fixture(workqueue.AllowPartial)
	key := view.Members[0].Group
	ds, err := b.Evaluate(context.Background(), key)
	if err != nil || len(ds) != 1 || ds[0].Unit != nil || ds[0].Action != staging.ActionSelect {
		t.Fatalf("无本地 Unit 时也应建议评估新成员: %+v %v", ds, err)
	}
	u := queuedUnit(t, view, "unit", workqueue.AllowPartial, 0)
	if err := b.Accept(context.Background(), u); err != nil {
		t.Fatal(err)
	}
	if err := b.Observe(context.Background(), staging.Observation{Group: key}); err != nil {
		t.Fatal(err)
	}
	view.Group.Generation++
	ds, err = b.Evaluate(context.Background(), key)
	if err != nil || ds[0].Reason != "AttemptInvalidated" || ds[0].Operation != staging.OperationRecover {
		t.Fatalf("声明变化后不能直接执行旧轮次: %+v %v", ds, err)
	}
}

func TestEvaluateBoundPodCompletionIsNotSchedulingFailure(t *testing.T) {
	b, view := fixture(workqueue.AllTogether)
	u := queuedUnit(t, view, "unit", workqueue.AllTogether, 0, 1)
	if err := b.Accept(context.Background(), u); err != nil {
		t.Fatal(err)
	}
	for _, member := range view.Members {
		member.Pod.Spec.NodeName = "node-a"
	}
	// 一个成员很快跑完，另一个仍在运行，不能据此要求整组回滚。
	view.Members[0].Pod.Status.Phase = corev1.PodSucceeded
	if err := b.Observe(context.Background(), staging.Observation{Group: u.Key().Group}); err != nil {
		t.Fatal(err)
	}
	ds, err := b.Evaluate(context.Background(), u.Key().Group)
	if err != nil || len(ds) != 1 || ds[0].Action != staging.ActionWait || ds[0].Reason != "MembersBoundAwaitingSettlement" {
		t.Fatalf("业务完成不能误判为调度失败: %+v %v", ds, err)
	}
}

func TestEvaluationQueueDeduplicatesAndRequeuesChangesDuringProcessing(t *testing.T) {
	b, view := fixture(workqueue.AllTogether)
	u := queuedUnit(t, view, "unit", workqueue.AllTogether, 0, 1)
	if err := b.Accept(context.Background(), u); err != nil {
		t.Fatal(err)
	}
	for n := 0; n < 10; n++ {
		if err := b.Observe(context.Background(), staging.Observation{Group: u.Key().Group}); err != nil {
			t.Fatal(err)
		}
	}
	if b.evaluations.Len() != 1 {
		t.Fatal("重复组通知未去重")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	entered, release, second := make(chan struct{}), make(chan struct{}), make(chan struct{}, 2)
	done := make(chan error, 1)
	var calls atomic.Int32
	go func() {
		done <- b.RunEvaluations(ctx, 8, func(ctx context.Context, ds []staging.Decision) error {
			if calls.Add(1) == 1 {
				close(entered)
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
			} else {
				select {
				case second <- struct{}{}:
				default:
				}
			}
			return nil
		})
	}()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("worker: %v", err)
		}
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("worker 未启动")
	}
	for n := 0; n < 10; n++ {
		if err := b.Observe(ctx, staging.Observation{Group: u.Key().Group}); err != nil {
			t.Fatal(err)
		}
	}
	close(release)
	select {
	case <-second:
	case <-ctx.Done():
		t.Fatal("处理期间的通知丢失")
	}
	// 新一轮回调正常完成后，队列应安静等待，worker 的观察刷新不能自触发忙循环。
	deadline := time.NewTimer(30 * time.Millisecond)
	defer deadline.Stop()
	select {
	case <-second:
		t.Fatal("没有新通知却重复评估")
	case <-deadline.C:
	}
}
