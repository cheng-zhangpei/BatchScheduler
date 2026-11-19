package buffer

import (
	"context"
	"testing"

	"batchscheduler/src/group"
	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// 模拟 Informer 先看到 A，稍后才看到 B。队列不等齐，Buffer 也不能提前执行 A。
func TestAtomicBatchesJoinInBufferBeforeExecution(t *testing.T) {
	b, view := fixture(t, workqueue.AllTogether)
	all := view.Members
	view.Members = all[:1]
	view.Members[0].Pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodScheduled,
		Status: corev1.ConditionFalse, Reason: corev1.PodReasonUnschedulable}}
	w := workqueue.New()
	w.Views, w.Staging = b.views, b
	ctx := context.Background()
	if err := w.Select(workqueue.SelectRequest{View: view}); err != nil {
		t.Fatal(err)
	}
	first := acceptActive(t, ctx, w, b)
	if d := decisionFor(t, b, first.Key()); d.Action != staging.ActionWait || d.Reason != "AwaitingGroupMembers" {
		t.Fatalf("成员未齐时不应启动执行: %+v", d)
	}

	// 等齐期间即使观察到旧调度失败，也不能提前启动一轮恢复而拒绝后到的 B。
	view.Members[0].Pod.Status.Conditions = nil
	view.Members = all
	if err := w.Select(workqueue.SelectRequest{View: view}); err != nil {
		t.Fatal(err)
	}
	second := acceptActive(t, ctx, w, b)
	r := b.records[first.Key()]
	if len(b.records) != 1 || len(r.Members) != 2 || len(r.Unit.Members()) != 2 {
		t.Fatal("两批成员没有汇入一份完整 Atomic Record")
	}
	for _, member := range all {
		if b.owners[first.Key().Group][member.Pod.UID] != first.Key() {
			t.Fatal("合并后的成员没有归到主 Record")
		}
	}
	// 交接重复到达，仍按每一批原始名单识别，不能再合一次或改掉执行进度。
	for _, unit := range []workqueue.Unit{first, second} {
		if err := b.Accept(ctx, unit); err != nil {
			t.Fatalf("重复接收不幂等: %v", err)
		}
	}
	started := 0
	b.handlers.Execute = func(_ context.Context, request staging.Execution) error {
		started++
		if len(request.Members) != 2 || request.Unit != first.Key() {
			t.Fatal("执行器没有收到合并后的完整组")
		}
		return nil
	}
	d := decisionFor(t, b, first.Key())
	if d.Action != staging.ActionExecute || d.Operation != staging.OperationRun {
		t.Fatalf("成员到齐后没有启动执行: %+v", d)
	}
	if err := b.Apply(ctx, d); err != nil {
		t.Fatal(err)
	}
	if next := decisionFor(t, b, first.Key()); next.Action != staging.ActionWait || started != 1 {
		t.Fatal("同一组被重复启动")
	}
}

func TestAtomicAssemblyRejectsOverlappingBatchWithoutChangingRecord(t *testing.T) {
	b, view := fixture(t, workqueue.AllTogether)
	first := queuedUnit(t, view, "first", workqueue.AllTogether, 0)
	if err := b.Accept(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	overlap := queuedUnit(t, view, "overlap", workqueue.AllTogether, 0, 1)
	if err := b.Accept(context.Background(), overlap); err == nil {
		t.Fatal("重叠成员不应再次汇入")
	}
	r := b.records[first.Key()]
	if len(r.Members) != 1 || len(r.Unit.Members()) != 1 || len(b.owners[first.Key().Group]) != 1 {
		t.Fatal("拒绝重叠批次时破坏了原 Record")
	}
}

func TestAtomicWaitingDoesNotHideGroupDeletion(t *testing.T) {
	b, view := fixture(t, workqueue.AllTogether)
	first := queuedUnit(t, view, "first", workqueue.AllTogether, 0)
	if err := b.Accept(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	now := metav1.Now()
	view.Group.DeletionTimestamp = &now
	d := decisionFor(t, b, first.Key())
	if d.Action != staging.ActionExecute || d.Operation != staging.OperationRecover {
		t.Fatalf("组已删除，不能继续等待成员: %+v", d)
	}
}

func TestPartialExecutionKeepsRetryCountsAfterQueueTransfer(t *testing.T) {
	b, view := fixture(t, workqueue.AllowPartial)
	w := workqueue.New()
	w.Views, w.Staging = b.views, b
	if err := w.Select(workqueue.SelectRequest{View: view}); err != nil {
		t.Fatal(err)
	}
	first := acceptActive(t, context.Background(), w, b)
	// 按真实 Handoff 接口把一批资源失败的成员交回，不要求队列保留旧 Unit。
	refs := first.Members()
	if err := w.Select(workqueue.SelectRequest{Return: &workqueue.ReturnRequest{
		Source: first, Members: refs[1:], KeepPrevious: true, RecoveryComplete: true,
		To: workqueue.QueueRetry, Failure: &group.FailureInfo{Reason: workqueue.ReasonInsufficientResources},
	}}); err != nil {
		t.Fatal(err)
	}
	queued := w.GroupSnapshot(first.Key().Group)
	if len(queued) != 1 || queued[0].Unit.State().FailedAttempts[refs[1].UID] != 1 {
		t.Fatal("交接后重试次数丢失，或未创建新的 Retry Unit")
	}
	if len(b.records[first.Key()].Members) != 2 {
		t.Fatal("Select 越过 Buffer 修改了原执行记录")
	}
}
