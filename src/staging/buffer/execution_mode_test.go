package buffer

import (
	"context"
	"testing"

	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
)

func TestExecutePassesFrozenUnitMode(t *testing.T) {
	for _, mode := range []workqueue.SubmitMode{workqueue.AllTogether, workqueue.AllowPartial} {
		t.Run(string(mode), func(t *testing.T) {
			b, view := fixture(mode)
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
		})
	}
}
