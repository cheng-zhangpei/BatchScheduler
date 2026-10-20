package staging

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"batchscheduler/src/group"
	"batchscheduler/src/workqueue"
)

func decisionFor(t *testing.T, b *Buffer, key workqueue.UnitKey) Decision {
	t.Helper()
	if err := b.Observe(context.Background(), Observation{Group: key.Group}); err != nil {
		t.Fatal(err)
	}
	ds, err := b.Evaluate(context.Background(), key.Group)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range ds {
		if d.Unit != nil && *d.Unit == key {
			return d
		}
	}
	t.Fatal("没有找到本轮决策")
	return Decision{}
}

func simulatedExecutor(b *Buffer, view *group.View, fail bool) func(context.Context, Execution) error {
	return func(ctx context.Context, request Execution) error {
		feedback := Feedback{Unit: request.Unit, ActionID: request.ActionID, Operation: request.Operation}
		switch request.Operation {
		case OperationSchedule:
			if fail {
				feedback.Failure = &group.FailureInfo{Reason: workqueue.ReasonInsufficientResources}
			} else {
				feedback.Plan = make(map[workqueue.PodRef]string)
				for _, ref := range request.Members {
					feedback.Plan[ref] = "node-a"
				}
			}
		case OperationBind:
			for _, member := range view.Members {
				member.Pod.Spec.NodeName = request.Plan[workqueue.PodRef{Name: member.Pod.Name, UID: member.Pod.UID}]
			}
			if err := b.Observe(ctx, Observation{Group: request.Unit.Group}); err != nil {
				return err
			}
			feedback.ResponsibilitiesSettled = true
		case OperationRecover:
			feedback.ResponsibilitiesSettled = true
		}
		return b.Report(ctx, feedback)
	}
}

func TestApplyMockSuccessClosesCenterAndReleasesIndexes(t *testing.T) {
	b, view := fixture(workqueue.AllTogether)
	u := queuedUnit(t, view, "unit", workqueue.AllTogether, 0, 1)
	b.handlers.Execute = simulatedExecutor(b, view, false)
	handoffs := 0
	b.handlers.Handoff = func(_ context.Context, h Handoff) error {
		if h.Decision.Action != ActionRelease || !b.ContainsPod(u.Key().Group, u.Members()[0].UID) {
			t.Fatal("交接确认前不能提前释放")
		}
		handoffs++
		return nil // 模拟队列侧已解除旧归属；不冒充真实 workqueue 适配器。
	}
	if err := b.Accept(context.Background(), u); err != nil {
		t.Fatal(err)
	}
	for _, op := range []Operation{OperationSchedule, OperationAssume, OperationBind, ""} {
		d := decisionFor(t, b, u.Key())
		if d.Operation != op {
			t.Fatalf("unexpected stage: %+v", d)
		}
		if err := b.Apply(context.Background(), d); err != nil {
			t.Fatal(err)
		}
	}
	if handoffs != 1 || len(b.records) != 0 || len(b.groups) != 0 || len(b.owners) != 0 || u.State().Phase != workqueue.UnitDone {
		t.Fatal("闭环结束后记录或索引未释放")
	}
	if err := b.Accept(context.Background(), u); err == nil {
		t.Fatal("结束的旧 Unit 不能被重新接收")
	}
}

func TestApplyMockFailureRecoveryAndReturn(t *testing.T) {
	for _, mode := range []workqueue.SubmitMode{workqueue.AllTogether, workqueue.AllowPartial} {
		t.Run(string(mode), func(t *testing.T) {
			b, view := fixture(mode)
			indices := []int{0}
			if mode == workqueue.AllTogether {
				indices = append(indices, 1)
			}
			u := queuedUnit(t, view, "unit", mode, indices...)
			b.handlers.Execute = simulatedExecutor(b, view, true)
			returned := false
			b.handlers.Handoff = func(_ context.Context, h Handoff) error {
				if h.Decision.Action != ActionReturn || h.Decision.To != workqueue.QueueFail || h.Decision.Failure.Reason != workqueue.ReasonInsufficientResources {
					t.Fatalf("错误的回送: %+v", h)
				}
				if len(h.Decision.Members) != len(indices) {
					t.Fatal("恢复回送范围错误")
				}
				returned = true
				return nil
			}
			if err := b.Accept(context.Background(), u); err != nil {
				t.Fatal(err)
			}
			for _, action := range []Action{ActionExecute, ActionExecute, ActionReturn} {
				d := decisionFor(t, b, u.Key())
				if d.Action != action {
					t.Fatalf("unexpected decision: %+v", d)
				}
				if err := b.Apply(context.Background(), d); err != nil {
					t.Fatal(err)
				}
			}
			if !returned || len(b.records) != 0 {
				t.Fatal("恢复后的模拟交接没有闭合")
			}
		})
	}
}

func TestApplyRejectsStaleAndConcurrentDuplicateDispatch(t *testing.T) {
	b, view := fixture(workqueue.AllTogether)
	u := queuedUnit(t, view, "unit", workqueue.AllTogether, 0, 1)
	var calls atomic.Int32
	b.handlers.Execute = func(context.Context, Execution) error { calls.Add(1); return nil }
	if err := b.Accept(context.Background(), u); err != nil {
		t.Fatal(err)
	}
	d := decisionFor(t, b, u.Key())
	var wg sync.WaitGroup
	errs := make(chan error, 12)
	for n := 0; n < 12; n++ {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- b.Apply(context.Background(), d) }()
	}
	wg.Wait()
	close(errs)
	success := 0
	for err := range errs {
		if err == nil {
			success++
		} else if !errors.Is(err, ErrStaleDecision) {
			t.Fatal(err)
		}
	}
	if success != 1 || calls.Load() != 1 {
		t.Fatal("重复决策启动了多次执行")
	}
	if err := b.Report(context.Background(), Feedback{Unit: u.Key(), ActionID: "wrong", Operation: OperationSchedule}); err == nil {
		t.Fatal("迟到/伪造身份不能覆盖在途操作")
	}
	if decisionFor(t, b, u.Key()).Action != ActionWait {
		t.Fatal("未反馈的操作应等待")
	}

	b2, v2 := fixture(workqueue.AllTogether)
	u2 := queuedUnit(t, v2, "unit", workqueue.AllTogether, 0, 1)
	b2.handlers.Execute = b.handlers.Execute
	if err := b2.Accept(context.Background(), u2); err != nil {
		t.Fatal(err)
	}
	old := decisionFor(t, b2, u2.Key())
	v2.Members[0].Pod.Spec.NodeName = "other-node"
	if err := b2.Apply(context.Background(), old); !errors.Is(err, ErrStaleDecision) {
		t.Fatalf("执行前必须重查缓存: %v", err)
	}
}

func TestApplyUnknownBindingAndSettlementKeepResponsibility(t *testing.T) {
	b, view := fixture(workqueue.AllTogether)
	u := queuedUnit(t, view, "unit", workqueue.AllTogether, 0, 1)
	execute := simulatedExecutor(b, view, false)
	var binding Execution
	b.handlers.Execute = func(ctx context.Context, req Execution) error {
		if req.Operation != OperationBind {
			return execute(ctx, req)
		}
		binding = req
		return b.Report(ctx, Feedback{Unit: req.Unit, ActionID: req.ActionID, Operation: req.Operation, Unknown: true})
	}
	if err := b.Accept(context.Background(), u); err != nil {
		t.Fatal(err)
	}
	for n := 0; n < 3; n++ {
		if err := b.Apply(context.Background(), decisionFor(t, b, u.Key())); err != nil {
			t.Fatal(err)
		}
	}
	for _, member := range view.Members {
		member.Pod.Spec.NodeName = "node-a"
	}
	if d := decisionFor(t, b, u.Key()); d.Action != ActionWait {
		t.Fatalf("未知结果不能因已观察绑定而提前清理: %+v", d)
	}
	feedback := Feedback{Unit: u.Key(), ActionID: binding.ActionID, Operation: OperationBind}
	if err := b.Report(context.Background(), feedback); err != nil {
		t.Fatal(err)
	}
	if d := decisionFor(t, b, u.Key()); d.Action != ActionWait {
		t.Fatal("账目未结清应继续等待")
	}
	feedback.ResponsibilitiesSettled = true
	if err := b.Report(context.Background(), feedback); err != nil {
		t.Fatal(err)
	}
	if err := b.Report(context.Background(), feedback); err != nil {
		t.Fatal("相同反馈重放应安全", err)
	}
	d := decisionFor(t, b, u.Key())
	if d.Action != ActionRelease {
		t.Fatalf("最终确认后应允许清理: %+v", d)
	}
	if err := b.Apply(context.Background(), d); !errors.Is(err, ErrHandlerUnavailable) || !b.ContainsPod(u.Key().Group, u.Members()[0].UID) {
		t.Fatal("未接交接适配器时不能静默清理")
	}
	// 明确未提交的交接失败可以保留记录后重试；与未知结果保留在途标记不同。
	b.handlers.Handoff = func(context.Context, Handoff) error { return errors.New("not committed") }
	if err := b.Apply(context.Background(), d); err == nil || b.records[u.Key()].PendingAction != "" {
		t.Fatal("明确未提交的失败应允许重评交接")
	}
	d = decisionFor(t, b, u.Key())
	var handoff Handoff
	b.handlers.Handoff = func(_ context.Context, h Handoff) error { handoff = h; return ErrHandoffUnknown }
	if err := b.Apply(context.Background(), d); !errors.Is(err, ErrHandoffUnknown) {
		t.Fatal(err)
	}
	if err := b.ConfirmHandoff(context.Background(), u.Key(), "wrong", true); err == nil {
		t.Fatal("交接确认也必须核对身份")
	}
	if !b.ContainsPod(u.Key().Group, u.Members()[0].UID) {
		t.Fatal("未知交接结果不能丢归属")
	}
	if err := b.ConfirmHandoff(context.Background(), u.Key(), handoff.ActionID, true); err != nil {
		t.Fatal(err)
	}
	if len(b.records) != 0 {
		t.Fatal("确认交接后没有释放")
	}
}

func TestApplyGroupSelectionUsesRealWorkQueue(t *testing.T) {
	b, view := fixture(workqueue.AllowPartial)
	w := workqueue.New()
	w.Views, w.Staging = b.views, b
	b.handlers.SelectGroup = func(_ context.Context, v *group.View) error { return w.Select(workqueue.SelectRequest{View: v}) }
	ds, err := b.Evaluate(context.Background(), view.Members[0].Group)
	if err != nil {
		t.Fatal(err)
	}
	for n := 0; n < 2; n++ {
		if err := b.Apply(context.Background(), ds[0]); err != nil {
			t.Fatal(err)
		}
	}
	for n := 0; n < 2; n++ {
		u, err := w.Active.Pop(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Accept(context.Background(), u); err != nil {
			t.Fatal(err)
		}
	}
	if len(b.records) != 2 {
		t.Fatal("重复组级决策不应创建额外 Unit")
	}
}
