package buffer

import (
	"context"
	"testing"
	"time"

	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
)

func TestReleaseHandoffWithRealWorkQueue(t *testing.T) {
	b, view := fixture(t, workqueue.AllowPartial)
	w := workqueue.New()
	w.Views, w.Staging = b.views, b
	if err := w.Select(workqueue.SelectRequest{View: view}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	u := acceptActive(t, ctx, w, b)
	b.handlers.Execute = simulatedExecutor(b, view, false)
	b.handlers.Handoff = func(_ context.Context, request staging.Handoff) error {
		if request.Decision.Action != staging.ActionRelease {
			t.Fatal("只应进入 Release 分支")
		}
		if !b.ContainsPod(u.Key().Group, u.Members()[0].UID) {
			t.Fatal("队列处理前 Buffer 已提前删记录")
		}
		return nil // 队列关联在 DispatchActive 时已移除，Release 只结束 Buffer 记录。
	}
	for _, op := range []staging.Operation{staging.OperationRun, ""} {
		d := decisionFor(t, b, u.Key())
		if d.Operation != op {
			t.Fatalf("错误阶段: %+v", d)
		}
		if err := b.Apply(ctx, d); err != nil {
			t.Fatal(err)
		}
	}
	if len(b.records) != 0 || len(b.groups) != 0 || len(b.owners) != 0 || u.State().Phase != workqueue.UnitDone {
		t.Fatal("真实队列清理后，Buffer 没有完成本轮清理")
	}
}

func acceptActive(t *testing.T, ctx context.Context, w *workqueue.WorkQueue, b *Buffer) workqueue.Unit {
	t.Helper()
	var received workqueue.Unit
	if err := w.DispatchActive(ctx, func(ctx context.Context, unit workqueue.Unit) error {
		received = unit
		return b.Accept(ctx, unit)
	}); err != nil {
		t.Fatal(err)
	}
	return received
}
