package executor

import (
	"context"
	"testing"

	"batchscheduler/src/group"
	"batchscheduler/src/scheduler/resourcecache"
	"batchscheduler/src/scheduler/steps"
	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestRecoveryReportsResourceOutcomeAndOriginalFailure(t *testing.T) {
	cases := []struct {
		name      string
		assumed   bool
		binding   bool
		bound     bool
		wantKind  staging.FeedbackKind
		wantPhase staging.ExecutionPhase
		wantCPU   int64
	}{
		{"before-assume", false, false, false, staging.FeedbackFinished, staging.PhaseRecovered, 0},
		{"forget-succeeded", true, false, false, staging.FeedbackFinished, staging.PhaseRecovered, 0},
		{"binding-unknown", true, true, false, staging.FeedbackUnknown, staging.PhaseRecovering, 1000},
		{"partial-binding-blocked", true, false, true, staging.FeedbackBlocked, staging.PhaseRecovering, 1000},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			resources := resourcecache.New()
			resources.UpdateNode(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node"}, Status: corev1.NodeStatus{Allocatable: corev1.ResourceList{
				corev1.ResourceCPU: resource.MustParse("4"), corev1.ResourcePods: resource.MustParse("32"),
			}}})
			unit := workqueue.UnitKey{Group: group.Key{Namespace: "work", Name: "job", UID: "group"}, ID: "unit"}
			ref := workqueue.PodRef{Name: "pod", UID: "pod"}
			a := &executionAttempt{Attempt: steps.Attempt{
				Request: staging.Execution{Unit: unit, ActionID: "action", Operation: staging.OperationRun, Members: []workqueue.PodRef{ref}},
				Plan:    map[workqueue.PodRef]string{ref: "node"}, Reserved: tc.assumed, Binding: tc.binding,
			}, failure: &group.FailureInfo{Reason: workqueue.ReasonUnschedulable, Message: "原始失败原因"}}
			if tc.assumed {
				pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "work", Name: ref.Name, UID: ref.UID}, Spec: corev1.PodSpec{
					NodeName: "node", Containers: []corev1.Container{{Name: "app", Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")},
					}}},
				}}
				if err := resources.Assume([]resourcecache.Assumption{{Unit: unit, ActionID: "action", Pod: pod}}); err != nil {
					t.Fatal(err)
				}
				a.AssumedMembers = []workqueue.PodRef{ref}
			}
			if tc.bound {
				a.BoundMembers = []workqueue.PodRef{ref}
			}
			e := NewExecutor(nil, resources, DefaultConfig())
			var reports []staging.Feedback
			e.Report = func(_ context.Context, f staging.Feedback) error {
				reports = append(reports, f)
				return nil
			}
			if err := e.recoverAttempt(ctx, a); err != nil {
				t.Fatal(err)
			}
			if len(reports) != 2 || reports[0].Kind != staging.FeedbackProgress || reports[0].Phase != staging.PhaseRecovering {
				t.Fatalf("应先汇报恢复进展，再汇报结果: %+v", reports)
			}
			last := reports[1]
			if last.Kind != tc.wantKind || last.Phase != tc.wantPhase || last.ResponsibilitiesSettled != (tc.wantKind == staging.FeedbackFinished) {
				t.Fatalf("恢复结果没有正确传回: %+v", last)
			}
			if last.Unit != unit || last.ActionID != "action" || last.Failure == nil || *last.Failure != *a.failure {
				t.Fatalf("反馈丢失执行身份或原始失败分类: %+v", last)
			}
			if got := resources.Snapshot().Nodes["node"].GetRequested().GetMilliCPU(); got != tc.wantCPU {
				t.Fatalf("恢复后占用 = %d，期望 %d", got, tc.wantCPU)
			}
		})
	}
}
