package buffer

import (
	"context"
	"path/filepath"
	"testing"

	"batchscheduler/src/staging"
	"batchscheduler/src/staging/history"
	"batchscheduler/src/workqueue"
)

func TestReleaseArchivesBeforeHandoffAndContextSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	b, view := fixture(t, workqueue.AllowPartial)
	path := filepath.Join(t.TempDir(), "history.db")
	store, err := history.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	b.history = store
	first := queuedUnit(t, view, "first", workqueue.AllowPartial, 0)
	second := queuedUnit(t, view, "second", workqueue.AllowPartial, 1)
	for _, unit := range []workqueue.Unit{first, second} {
		if err := b.Accept(ctx, unit); err != nil {
			t.Fatal(err)
		}
	}
	b.handlers.Execute = simulatedExecutor(b, view, false)
	b.handlers.Handoff = func(_ context.Context, request staging.Handoff) error {
		group, err := b.GroupContext(ctx, first.Key().Group)
		if err != nil || len(group.Succeeded) != 1 || len(group.InFlight) != 1 || group.InFlight[0].Unit != second.Key() {
			t.Fatal("交接前应已保存成功记录，同一份记录不能同时出现两次", err)
		}
		if !b.ContainsPod(first.Key().Group, first.Members()[0].UID) {
			t.Fatal("落盘后仍应等队列交接成功，再清理活跃归属")
		}
		return nil
	}
	for range 2 {
		if err := b.Apply(ctx, decisionFor(t, b, first.Key())); err != nil {
			t.Fatal(err)
		}
	}
	if b.ContainsPod(first.Key().Group, first.Members()[0].UID) || len(b.records) != 1 {
		t.Fatal("成功记录应离开活跃区，其他 Unit 保留")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = history.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	restarted := NewBuffer(b.views, store)
	group, err := restarted.GroupContext(ctx, first.Key().Group)
	if err != nil || len(group.Succeeded) != 1 || group.Succeeded[0].Members[0].Observed.NodeName != "node-a" || len(group.InFlight) != 0 {
		t.Fatal("重启后仍应能查到历史成功位置；本次不恢复未结束任务", err)
	}
}

func TestArchiveFailureKeepsRecordAndDoesNotTouchQueue(t *testing.T) {
	ctx := context.Background()
	b, view := fixture(t, workqueue.AllowPartial)
	u := queuedUnit(t, view, "unit", workqueue.AllowPartial, 0)
	if err := b.Accept(ctx, u); err != nil {
		t.Fatal(err)
	}
	b.handlers.Execute = simulatedExecutor(b, view, false)
	handoffs := 0
	b.handlers.Handoff = func(context.Context, staging.Handoff) error { handoffs++; return nil }
	if err := b.Apply(ctx, decisionFor(t, b, u.Key())); err != nil {
		t.Fatal(err)
	}
	if err := b.history.(*history.Store).Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.Apply(ctx, decisionFor(t, b, u.Key())); err == nil {
		t.Fatal("磁盘保存失败不能宣称释放成功")
	}
	if handoffs != 0 || b.records[u.Key()] == nil || b.records[u.Key()].PendingAction != "" || u.State().Phase != workqueue.UnitProcessing {
		t.Fatal("保存失败时丢了 Record、动了队列或留下无法重试的 Pending")
	}
}

func TestInvalidatedReleaseDoesNotCreateSuccessHistory(t *testing.T) {
	ctx := context.Background()
	b, view := fixture(t, workqueue.AllowPartial)
	u := queuedUnit(t, view, "unit", workqueue.AllowPartial, 0)
	if err := b.Accept(ctx, u); err != nil {
		t.Fatal(err)
	}
	b.records[u.Key()].ResponsibilitiesSettled = true
	view.Group.Generation++
	b.handlers.Handoff = func(context.Context, staging.Handoff) error { return nil }
	d := decisionFor(t, b, u.Key())
	if d.Action != staging.ActionRelease || d.Reason != "AttemptInvalidated" {
		t.Fatal("测试应走失效清理路径")
	}
	if err := b.Apply(ctx, d); err != nil {
		t.Fatal(err)
	}
	if records, err := b.history.List(ctx, u.Key().Group); err != nil || len(records) != 0 {
		t.Fatal("失效清理被错误存成调度成功", err)
	}
}
