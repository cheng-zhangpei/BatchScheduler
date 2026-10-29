package executor

import (
	"context"
	"errors"
	"testing"

	scheduling "batchscheduler/src/apis/scheduling/v1alpha1"
	"batchscheduler/src/group"
	"batchscheduler/src/scheduler/resourcecache"
	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestRunStopsWhenBufferRejectsProgress(t *testing.T) {
	e := NewExecutor(nil, nil, DefaultConfig())
	want := errors.New("buffer rejected progress")
	calls := 0
	e.Report = func(_ context.Context, f staging.Feedback) error {
		calls++
		if f.Phase != staging.PhaseFiltering {
			t.Fatalf("反馈被拒绝后仍进入了恢复或结束流程: %+v", f)
		}
		return want
	}
	_, err := e.prepareExecution(context.Background(), staging.Execution{Operation: staging.OperationRun})
	if !errors.Is(err, want) || calls != 1 {
		t.Fatalf("汇报失败应直接停止，不应伪装成调度失败: calls=%d err=%v", calls, err)
	}
}

type executionViews struct{ view *group.View }

func (v executionViews) Get(string, string) (*group.View, error) { return v.view, nil }

func TestRunKeepsReservationWhenAssumedReportFails(t *testing.T) {
	key := group.Key{Namespace: "work", Name: "job", UID: "group"}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: "pod", UID: "pod"},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")},
		}}}}}
	view := &group.View{Group: &scheduling.PodGroup{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name, UID: key.UID}},
		Members: []group.PodView{{Group: key, Pod: pod}}}
	resources := resourcecache.New()
	resources.UpdateNode(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}, Status: corev1.NodeStatus{Allocatable: corev1.ResourceList{
		corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourcePods: resource.MustParse("32"),
	}}})
	e := NewExecutor(executionViews{view}, resources, DefaultConfig())
	want := errors.New("Assumed report rejected")
	last := staging.ExecutionPhase("")
	e.Report = func(_ context.Context, f staging.Feedback) error {
		last = f.Phase
		if f.Phase == staging.PhaseAssumed {
			return want
		}
		return nil
	}
	_, err := e.prepareExecution(context.Background(), staging.Execution{Unit: workqueue.UnitKey{Group: key, ID: "unit"}, ActionID: "action",
		Operation: staging.OperationRun, Mode: workqueue.AllTogether, Members: []workqueue.PodRef{{Name: pod.Name, UID: pod.UID}}})
	if !errors.Is(err, want) || last != staging.PhaseAssumed || resources.Snapshot().Nodes["node-a"].GetRequested().GetMilliCPU() != 1000 {
		t.Fatalf("Report 失败被当成业务失败继续绑定或退账: last=%s, err=%v", last, err)
	}
}
