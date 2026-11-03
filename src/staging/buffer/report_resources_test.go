package buffer

import (
	"context"
	"errors"
	"testing"

	"batchscheduler/src/group"
	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
)

func TestEvaluateUnplacedPreservesRunningMembers(t *testing.T) {
	for _, tc := range []struct {
		reason string
		queue  workqueue.QueueName
	}{
		{workqueue.ReasonUnschedulable, workqueue.QueueRetry},
		{workqueue.ReasonForbidden, workqueue.QueueFail},
		{workqueue.ReasonSchedulingError, workqueue.QueueFail},
	} {
		t.Run(tc.reason, func(t *testing.T) {
			b, f := startPartialResourceReport(t)
			f.Resources.Unplaced[0].Failure.Reason = tc.reason
			if err := b.Report(context.Background(), f); err != nil {
				t.Fatal(err)
			}
			r := b.records[f.Unit]
			revision := r.Revision
			d := decisionFor(t, b, f.Unit)
			if d.Action != staging.ActionReturnMembers || d.To != tc.queue || len(d.Members) != 1 ||
				d.Members[0] != f.Resources.Unplaced[0].Pod || d.Failure == nil || d.Failure.Reason != tc.reason {
				t.Fatalf("没有生成针对未放置成员的回送决定: %+v", d)
			}
			if r.Revision != revision || r.PendingAction != f.ActionID || len(r.Unit.Members()) != 2 || len(b.owners[f.Unit.Group]) != 2 {
				t.Fatal("生成决定时修改了执行记录或成员归属")
			}
			// 这里未配置 Handoff，不能误用整轮清理逻辑结束仍在执行的成员。
			if err := b.Apply(context.Background(), d); !errors.Is(err, staging.ErrHandlerUnavailable) {
				t.Fatalf("未配置交接函数时应明确保留任务: %v", err)
			}
			if b.records[f.Unit] != r || r.PendingAction != f.ActionID || len(b.owners[f.Unit.Group]) != 2 {
				t.Fatal("没有完成交接就结束了原轮次")
			}
			// 组声明已经改动，不能继续按旧接纳结果回送。
			b.views.(testViews).view.Group.Generation++
			if got := decisionFor(t, b, f.Unit); got.Action == staging.ActionReturnMembers {
				t.Fatal("组声明变更后仍按旧结果安排部分回送")
			}
		})
	}
}

func startPartialResourceReport(t *testing.T) (*Buffer, staging.Feedback) {
	t.Helper()
	b, view := fixture(t, workqueue.AllowPartial)
	u := queuedUnit(t, view, "unit", workqueue.AllowPartial, 0, 1)
	var request staging.Execution
	b.handlers.Execute = func(_ context.Context, r staging.Execution) error { request = r; return nil }
	if err := b.Accept(context.Background(), u); err != nil {
		t.Fatal(err)
	}
	if err := b.Apply(context.Background(), decisionFor(t, b, u.Key())); err != nil {
		t.Fatal(err)
	}
	// 原计划里两人都有位置，但本轮最新资源检查只接纳了第一人。
	return b, staging.Feedback{Unit: request.Unit, ActionID: request.ActionID, Operation: request.Operation,
		Kind: staging.FeedbackProgress, Phase: staging.PhaseAssumed,
		Plan: map[workqueue.PodRef]string{request.Members[0]: "node-a", request.Members[1]: "node-a"},
		Resources: &staging.ResourceResult{
			Assumed: []workqueue.PodRef{request.Members[0]},
			Unplaced: []staging.UnplacedMember{{Pod: request.Members[1], Failure: group.FailureInfo{
				Reason: workqueue.ReasonUnschedulable, Message: "本轮资源检查未通过",
			}}},
		}}
}

func TestReportStoresUnplacedWithoutEndingOrReturningUnit(t *testing.T) {
	b, f := startPartialResourceReport(t)
	if err := b.Report(context.Background(), f); err != nil {
		t.Fatal(err)
	}
	r := b.records[f.Unit]
	accepted, waiting := f.Resources.Assumed[0], f.Resources.Unplaced[0].Pod
	if r.Resources == nil || r.Resources.Unplaced[0].Failure.Reason != workqueue.ReasonUnschedulable {
		t.Fatal("未放置成员或错误分类没有存入 Record")
	}
	if r.PendingAction != f.ActionID || r.ResponsibilitiesSettled || len(r.Unit.Members()) != 2 || len(b.owners[f.Unit.Group]) != 2 {
		t.Fatal("进展报告提前结束执行、修改冻结成员或清除了成员归属")
	}
	if r.Members[accepted].Phase != group.PodReserved || r.Members[waiting].Phase != group.PodUnscheduled {
		t.Fatal("未放置成员被错误地记录为已经暂占")
	}
	// 执行器之后改自己的报告，不应修改 Buffer 保存的数据或重复反馈检查所用的副本。
	f.Resources.Assumed[0].Name = "changed"
	f.Resources.Unplaced[0].Failure.Reason = workqueue.ReasonForbidden
	if r.Resources.Assumed[0] != accepted || r.Resources.Unplaced[0].Failure.Reason != workqueue.ReasonUnschedulable ||
		r.LastFeedback.Resources.Unplaced[0].Failure.Reason != workqueue.ReasonUnschedulable {
		t.Fatal("Buffer 保留了执行器的可变名单")
	}
	// 后续不重复携带名单，也要沿用已保存的划分，不能把等待成员改成 Binding。
	if err := b.Report(context.Background(), staging.Feedback{Unit: f.Unit, ActionID: f.ActionID,
		Operation: f.Operation, Kind: staging.FeedbackProgress, Phase: staging.PhaseBinding}); err != nil {
		t.Fatal(err)
	}
	if r.Members[accepted].Phase != group.PodBinding || r.Members[waiting].Phase != group.PodUnscheduled || r.PendingAction != f.ActionID {
		t.Fatal("后续进度没有保留部分接纳的边界")
	}
}

func TestReportRejectsInvalidResourceResultWithoutChangingRecord(t *testing.T) {
	for _, kind := range []string{"missing-member", "duplicate-member", "foreign-member", "missing-reason", "atomic-partial"} {
		t.Run(kind, func(t *testing.T) {
			b, f := startPartialResourceReport(t)
			switch kind {
			case "missing-member":
				f.Resources.Unplaced = nil
			case "duplicate-member":
				f.Resources.Unplaced[0].Pod = f.Resources.Assumed[0]
			case "foreign-member":
				f.Resources.Unplaced[0].Pod.UID = "other-pod"
			case "missing-reason":
				f.Resources.Unplaced[0].Failure.Reason = ""
			case "atomic-partial":
				atomic, _, request := startReportedRun(t)
				b = atomic
				f.Unit, f.ActionID, f.Operation = request.Unit, request.ActionID, request.Operation
			}
			r := b.records[f.Unit]
			revision := r.Revision
			if err := b.Report(context.Background(), f); err == nil {
				t.Fatal("无效资源结果被接受")
			}
			if r.Resources != nil || r.Revision != revision || r.PendingAction != f.ActionID {
				t.Fatal("拒绝报告时仍然修改了 Record")
			}
		})
	}
}
