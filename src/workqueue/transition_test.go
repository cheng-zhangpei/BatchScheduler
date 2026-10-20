package workqueue

import (
	"errors"
	"sync"
	"testing"
	"time"

	"batchscheduler/src/group"
)

func failedUnit(t *testing.T, reason string, mode SubmitMode) (*WorkQueue, Unit) {
	t.Helper()
	count := 1
	if mode == AllTogether {
		count = 2
	}
	view := testView("work", "failed", mode, count, count)
	w := queueFor(view)
	w.Policy = func(SelectionInput, Unit) (QueueDecision, error) {
		return QueueDecision{To: QueueFail, Reason: reason, Message: "confirmed failure"}, nil
	}
	selectOK(t, w, SelectRequest{View: view})
	record := w.groups[view.Members[0].Group]
	return w, record.units[record.pods[view.Members[0].Pod.UID].UnitID]
}

func TestTransitionFailToRetryClassification(t *testing.T) {
	deadline := time.Unix(100, 0).UTC()
	for _, tc := range []struct {
		reason    string
		condition RetryCondition
		deadline  time.Time
	}{
		{ReasonInsufficientResources, RetryOnResources, time.Time{}},
		{ReasonNodeUnavailable, RetryOnNodes, time.Time{}},
		{ReasonPlacementConstraints, RetryOnPlacement, deadline},
		{ReasonDependencyNotReady, RetryOnDependency, time.Time{}},
		{ReasonTransientSchedulingError, RetryOnBackoff, deadline},
		{ReasonBindingRejected, RetryOnBackoff, deadline},
	} {
		t.Run(tc.reason, func(t *testing.T) {
			w, unit := failedUnit(t, tc.reason, AllTogether)
			err := w.Transition(TransitionRequest{Unit: unit, From: QueueFail, To: QueueRetry,
				RecoveryComplete: true, RetryAfter: tc.deadline})
			if err != nil {
				t.Fatal(err)
			}
			if _, exists := w.Fail.Get(unit.Key()); exists {
				t.Fatal("Fail 未移除")
			}
			retry, exists := w.Retry.Get(unit.Key())
			if !exists || retry.Unit != unit || retry.Plan.Condition != tc.condition || !retry.Plan.NotBefore.Equal(tc.deadline) {
				t.Fatalf("重试计划不匹配: %+v", retry)
			}
			state := unit.State()
			if state.Queue != QueueRetry || state.Phase != UnitQueued || state.LastFailure.Reason != tc.reason {
				t.Fatalf("状态未同步或失败记录丢失: %+v", state)
			}
			if (state.RetryAfter == nil) != tc.deadline.IsZero() {
				t.Fatal("退避时间未同步")
			}
			if state.RetryAfter != nil && !state.RetryAfter.Equal(tc.deadline) {
				t.Fatal("退避时间错误")
			}
			if len(unit.Members()) != 2 || !unit.Frozen() {
				t.Fatal("迁移改变了成员或冻结状态")
			}
			drainActive(t, w, 0) // 时间已过也不会由本路径自动激活。
		})
	}
}

func TestTransitionRejectsUnsafeRetriesWithoutMutation(t *testing.T) {
	for _, reason := range []string{ReasonBindingUnknown, ReasonInvalidSpec, ReasonForbidden,
		ReasonObjectDeleted, ReasonRecoveryPending, "UnclassifiedError", ReasonTransientSchedulingError, ReasonBindingRejected} {
		t.Run(reason, func(t *testing.T) {
			w, unit := failedUnit(t, reason, AllowPartial)
			if err := w.Transition(TransitionRequest{Unit: unit, From: QueueFail, To: QueueRetry, RecoveryComplete: true}); err == nil {
				t.Fatal("非自动重试类型或缺少退避时间应被拒绝")
			}
			if _, ok := w.Fail.Get(unit.Key()); !ok {
				t.Fatal("拒绝请求却丢失 Fail 条目")
			}
			if _, ok := w.Retry.Get(unit.Key()); ok {
				t.Fatal("拒绝请求却写入 Retry")
			}
			if unit.State().Queue != QueueFail {
				t.Fatal("拒绝请求却改变状态")
			}
		})
	}
}

func TestTransitionRequiresRecoveryAndCurrentSource(t *testing.T) {
	for _, kind := range []string{"recovery-pending", "reason-mismatch", "missing-source", "staged", "binding-unknown"} {
		t.Run(kind, func(t *testing.T) {
			w, unit := failedUnit(t, ReasonInsufficientResources, AllowPartial)
			request := TransitionRequest{Unit: unit, From: QueueFail, To: QueueRetry, RecoveryComplete: true}
			switch kind {
			case "recovery-pending":
				request.RecoveryComplete = false
			case "reason-mismatch":
				request.Reason = ReasonBindingRejected
			case "missing-source":
				w.Fail.Remove(unit.Key())
			case "staged":
				w.Staging = testStaging{key: unit.Key().Group, uid: unit.Members()[0].UID}
			case "binding-unknown":
				w.groups[unit.Key().Group].pods[unit.Members()[0].UID].Phase = group.PodBindingUnknown
			}
			if err := w.Transition(request); err == nil {
				t.Fatal("不满足前提的迁移应被拒绝")
			}
			if _, ok := w.Retry.Get(unit.Key()); ok {
				t.Fatal("拒绝请求却写入 Retry")
			}
			if unit.State().Queue != QueueFail {
				t.Fatal("拒绝请求却改变状态")
			}
		})
	}
}

func TestTransitionDuplicateRequestsKeepOriginalPlan(t *testing.T) {
	w, unit := failedUnit(t, ReasonTransientSchedulingError, AllowPartial)
	request := TransitionRequest{Unit: unit, From: QueueFail, To: QueueRetry,
		RecoveryComplete: true, RetryAfter: time.Unix(100, 0).UTC()}
	var wg sync.WaitGroup
	errs := make(chan error, 12)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- w.Transition(request) }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	request.RetryAfter = request.RetryAfter.Add(time.Minute)
	if err := w.Transition(request); err == nil {
		t.Fatal("重复请求不能覆盖等待计划")
	}
	retry, _ := w.Retry.Get(unit.Key())
	if !retry.Plan.NotBefore.Equal(time.Unix(100, 0)) {
		t.Fatal("原退避时间被覆盖")
	}
	if err := w.Transition(TransitionRequest{Unit: unit, From: QueueActive, To: QueueFail}); !errors.Is(err, ErrTransitionNotImplemented) {
		t.Fatalf("Active → Fail 不属于迁移路径: %v", err)
	}
	drainActive(t, w, 0)
}

func TestRetryWakeRequiresChangeAndDeadline(t *testing.T) {
	w, unit := failedUnit(t, ReasonInsufficientResources, AllTogether)
	deadline := time.Unix(100, 0)
	if err := w.Transition(TransitionRequest{Unit: unit, From: QueueFail, To: QueueRetry, RecoveryComplete: true, RetryAfter: deadline}); err != nil {
		t.Fatal(err)
	}
	wake := func(changed RetryCondition, now time.Time) error {
		return w.Transition(TransitionRequest{Unit: unit, From: QueueRetry, To: QueueActive, Wake: &RetryWake{Changed: changed, Now: now}})
	}
	if err := wake(RetryOnNodes, deadline); !errors.Is(err, ErrRetryNotReady) {
		t.Fatalf("无关事件不应激活: %v", err)
	}
	if err := wake(RetryOnResources, deadline.Add(-time.Second)); !errors.Is(err, ErrRetryNotReady) {
		t.Fatalf("退避未到不应激活: %v", err)
	}
	retry, ok := w.Retry.Get(unit.Key())
	if !ok || !retry.ChangeObserved {
		t.Fatal("退避期间的相关变化必须被记住")
	}
	if err := wake("", deadline); err != nil {
		t.Fatal(err)
	}
	if _, ok := w.Retry.Get(unit.Key()); ok {
		t.Fatal("激活后仍留在 Retry")
	}
	if state := unit.State(); state.Queue != QueueActive || state.RetryAfter != nil || state.WaitingFor != "" || state.LastFailure == nil {
		t.Fatalf("激活状态错误: %+v", state)
	}
	// 重复旧提示不能造成二次入队。
	if err := wake("", deadline); err == nil {
		t.Fatal("旧提示应该被拒绝")
	}
	drainActive(t, w, 1)
}

func TestRetryBackoffAndCurrentFacts(t *testing.T) {
	for _, kind := range []string{"timer", "no-event", "recreated", "bound", "staged", "binding-unknown"} {
		t.Run(kind, func(t *testing.T) {
			reason := ReasonInsufficientResources
			if kind == "timer" {
				reason = ReasonTransientSchedulingError
			}
			w, unit := failedUnit(t, reason, AllowPartial)
			deadline := time.Unix(100, 0)
			if err := w.Transition(TransitionRequest{Unit: unit, From: QueueFail, To: QueueRetry, RecoveryComplete: true, RetryAfter: deadline}); err != nil {
				t.Fatal(err)
			}
			view, err := w.Views.Get(unit.Key().Group.Namespace, unit.Key().Group.Name)
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "recreated":
				view.Members[0].Pod.UID = "replacement"
			case "bound":
				view.Members[0].Pod.Spec.NodeName = "node-a"
			case "staged":
				w.Staging = testStaging{key: unit.Key().Group, uid: unit.Members()[0].UID}
			case "binding-unknown":
				w.groups[unit.Key().Group].pods[unit.Members()[0].UID].Phase = group.PodBindingUnknown
			}
			changed := RetryOnResources
			if kind == "timer" || kind == "no-event" {
				changed = ""
			}
			err = w.Transition(TransitionRequest{Unit: unit, From: QueueRetry, To: QueueActive, Wake: &RetryWake{Changed: changed, Now: deadline}})
			if kind == "timer" {
				if err != nil {
					t.Fatal(err)
				}
				drainActive(t, w, 1)
			} else {
				if err == nil || unit.State().Queue != QueueRetry {
					t.Fatalf("无事件或失效成员不能激活: %v", err)
				}
				drainActive(t, w, 0)
			}
		})
	}
}
