package buffer

import (
	"context"
	"testing"

	scheduling "batchscheduler/src/apis/scheduling/v1alpha1"
	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
)

func TestExecutePassesFrozenUnitMode(t *testing.T) {
	for _, mode := range []workqueue.SubmitMode{workqueue.AllTogether, workqueue.AllowPartial} {
		t.Run(string(mode), func(t *testing.T) {
			b, view := fixture(t, mode)
			view.Group.Spec.Placement = &scheduling.PlacementSpec{ResourceConsolidation: true, Communication: []scheduling.CommunicationLink{{Name: "ab", From: "a", To: "b", Weight: 10}}}
			u := queuedUnit(t, view, "unit", mode, 0, 1)
			var received staging.Execution
			b.handlers.Execute = func(_ context.Context, request staging.Execution) error { received = request; return nil }
			if err := b.Accept(context.Background(), u); err != nil {
				t.Fatal(err)
			}
			if err := b.Apply(context.Background(), decisionFor(t, b, u.Key())); err != nil {
				t.Fatal(err)
			}
			if received.Mode != u.Mode() || received.Operation != staging.OperationRun {
				t.Fatal("Buffer 没有把冻结 Unit 的提交模式传给执行器")
			}
			if received.Definition == nil || received.Definition.Spec.Placement.Communication[0].Weight != 10 {
				t.Fatal("Buffer 没有把冻结组声明传给执行器")
			}
			received.Definition.Spec.Placement.Communication[0].Weight = 99
			if u.Definition().Spec.Placement.Communication[0].Weight != 10 {
				t.Fatal("执行参数修改影响了 Unit 的冻结声明")
			}
		})
	}
}
