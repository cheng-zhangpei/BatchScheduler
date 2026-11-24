package buffer

import (
	"context"
	"testing"

	"batchscheduler/src/group"
	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
)

func TestReportConfirmsUnknownBindingWithoutAllowingOtherPhaseReversal(t *testing.T) {
	for _, reason := range []string{workqueue.ReasonBindingUnknown, workqueue.ReasonForbidden} {
		t.Run(reason, func(t *testing.T) {
			b, _, request := startReportedRun(t)
			plan := make(map[workqueue.PodRef]string)
			for _, ref := range request.Members {
				plan[ref] = "node-a"
			}
			f := staging.Feedback{Unit: request.Unit, ActionID: request.ActionID, Operation: request.Operation,
				Kind: staging.FeedbackUnknown, Phase: staging.PhaseRecovering, Plan: plan,
				Failure: &group.FailureInfo{Reason: reason}}
			if err := b.Report(context.Background(), f); err != nil {
				t.Fatal(err)
			}
			f.Kind, f.Phase, f.Failure, f.ResponsibilitiesSettled = staging.FeedbackFinished, staging.PhaseBound, nil, true
			err := b.Report(context.Background(), f)
			if reason != workqueue.ReasonBindingUnknown {
				if err == nil {
					t.Fatal("非绑定未知错误也被允许跳回 Bound")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			r := b.records[request.Unit]
			if r.PendingAction != "" || r.Failure != nil || !r.ResponsibilitiesSettled {
				t.Fatalf("补确认后未结束未知状态: %+v", r)
			}
			for _, state := range r.Members {
				if state.Phase != group.PodBound {
					t.Fatal("成员未更新为已绑定")
				}
			}
		})
	}
}
