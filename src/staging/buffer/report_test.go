package buffer

import (
	"context"
	"testing"

	"batchscheduler/src/group"
	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
)

// 启动一轮但不模拟完成，让每个用例手动报告进度。
func startReportedRun(t *testing.T) (*Buffer, *group.View, staging.Execution) {
	t.Helper()
	b, view := fixture(t, workqueue.AllTogether)
	u := queuedUnit(t, view, "unit", workqueue.AllTogether, 0, 1)
	var request staging.Execution
	b.handlers.Execute = func(_ context.Context, r staging.Execution) error { request = r; return nil }
	if err := b.Accept(context.Background(), u); err != nil {
		t.Fatal(err)
	}
	if err := b.Apply(context.Background(), decisionFor(t, b, u.Key())); err != nil {
		t.Fatal(err)
	}
	return b, view, request
}

func TestReportProgressKeepsOneRunUntilFinished(t *testing.T) {
	b, view, request := startReportedRun(t)
	plan := make(map[workqueue.PodRef]string)
	for _, ref := range request.Members {
		plan[ref] = "node-a"
	}
	f := staging.Feedback{Unit: request.Unit, ActionID: request.ActionID, Operation: request.Operation, Kind: staging.FeedbackProgress}
	for _, phase := range []staging.ExecutionPhase{staging.PhaseFiltering, staging.PhaseFiltered, staging.PhaseScored, staging.PhasePlanned, staging.PhaseAssumed, staging.PhaseBinding, staging.PhaseBound} {
		f.Phase = phase
		if phase == staging.PhasePlanned {
			f.Plan = plan
		}
		if err := b.Report(context.Background(), f); err != nil {
			t.Fatal(err)
		}
		r := b.records[request.Unit]
		if r.PendingAction != request.ActionID || r.PendingOperation != staging.OperationRun || r.ExecutionPhase != phase {
			t.Fatalf("中间报告提前结束了本轮: %+v", r)
		}
		if phase == staging.PhaseAssumed {
			for ref, state := range r.Members {
				if state.Phase != group.PodReserved || state.PlannedNode != plan[ref] {
					t.Fatal("Assume 报告没有保存本轮进度和目标节点")
				}
			}
		}
		if d := decisionFor(t, b, request.Unit); d.Action != staging.ActionWait {
			t.Fatalf("Buffer 不应再派发 Assume/Bind: %+v", d)
		}
	}
	for _, member := range view.Members {
		member.Pod.Spec.NodeName = "node-a"
	}
	if d := decisionFor(t, b, request.Unit); d.Action != staging.ActionWait {
		t.Fatal("集群已绑定也不能替代执行器确认收尾")
	}
	f.Kind, f.ResponsibilitiesSettled = staging.FeedbackFinished, true
	if err := b.Report(context.Background(), f); err != nil {
		t.Fatal(err)
	}
	r := b.records[request.Unit]
	if r.PendingAction != "" || !r.ResponsibilitiesSettled {
		t.Fatal("最终报告没有结束本次等待")
	}
	if d := decisionFor(t, b, request.Unit); d.Action != staging.ActionRelease {
		t.Fatalf("整轮完成应允许 Release: %+v", d)
	}
}

func TestReportRejectsBadProgressWithoutChangingRecord(t *testing.T) {
	b, _, request := startReportedRun(t)
	plan := make(map[workqueue.PodRef]string)
	for _, ref := range request.Members {
		plan[ref] = "node-a"
	}
	f := staging.Feedback{Unit: request.Unit, ActionID: request.ActionID, Operation: request.Operation,
		Kind: staging.FeedbackProgress, Phase: staging.PhaseAssumed, Plan: plan}
	if err := b.Report(context.Background(), f); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"old-action", "phase-backward", "incomplete-plan", "change-after-assume", "finish-unsettled", "unknown-too-early"} {
		t.Run(kind, func(t *testing.T) {
			bad := f
			bad.Plan = copyPlan(plan)
			switch kind {
			case "old-action":
				bad.ActionID = "old"
			case "phase-backward":
				bad.Phase = staging.PhaseFiltered
			case "incomplete-plan":
				delete(bad.Plan, request.Members[0])
			case "change-after-assume":
				bad.Plan[request.Members[0]] = "node-b"
			case "finish-unsettled":
				bad.Phase, bad.Kind = staging.PhaseBound, staging.FeedbackFinished
			case "unknown-too-early":
				bad.Kind = staging.FeedbackUnknown
			}
			r := b.records[request.Unit]
			before := r.Revision
			if err := b.Report(context.Background(), bad); err == nil {
				t.Fatal("无效进度被接受")
			}
			if r.Revision != before || r.PendingAction != request.ActionID || r.ExecutionPhase != staging.PhaseAssumed || r.Plan[request.Members[0]] != "node-a" {
				t.Fatal("拒绝反馈时仍然修改了执行记录")
			}
		})
	}
}
