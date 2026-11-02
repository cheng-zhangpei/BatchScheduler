package steps

import (
	"context"
	"errors"
	"testing"

	"batchscheduler/src/group"
	"batchscheduler/src/scheduler/resourcecache"
	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func assumeStepFixture() (*Runner, *Attempt) {
	resources := resourcecache.New()
	resources.UpdateNode(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}, Status: corev1.NodeStatus{Allocatable: corev1.ResourceList{
		corev1.ResourceCPU: resource.MustParse("4"), corev1.ResourcePods: resource.MustParse("32"),
	}}})
	a := &Attempt{Request: staging.Execution{Unit: workqueue.UnitKey{Group: group.Key{Namespace: "work", Name: "job", UID: "group"}, ID: "unit"},
		ActionID: "action", Operation: staging.OperationRun}, Plan: make(map[workqueue.PodRef]string)}
	for _, name := range []string{"a", "b"} {
		pod := filterPod(name)
		pod.Spec.Containers = []corev1.Container{{Name: "app", Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")},
		}}}
		ref := workqueue.PodRef{Name: pod.Name, UID: pod.UID}
		a.members = append(a.members, pod)
		a.Request.Members = append(a.Request.Members, ref)
		a.Plan[ref] = "node-a"
	}
	return NewRunner(nil, resources), a
}

func TestAssumeStepAndRecoverUseSameExecution(t *testing.T) {
	r, a := assumeStepFixture()
	if err := r.Assume(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	if !a.Reserved || len(a.AssumedMembers) != 2 || r.resources.Snapshot().Nodes["node-a"].GetRequested().GetMilliCPU() != 4000 || a.members[0].Spec.NodeName != "" {
		t.Fatal("Assume 未写共享账、未保存资源责任，或改动了本轮原 Pod")
	}
	for range 2 {
		if err := r.Recover(context.Background(), a); err != nil {
			t.Fatal(err)
		}
	}
	if a.Reserved || len(a.AssumedMembers) != 0 || r.resources.Snapshot().Nodes["node-a"].GetRequested().GetMilliCPU() != 0 {
		t.Fatal("Recover 没有用本轮执行身份退账，或重复恢复多退资源")
	}
}

func TestAssumeFailureDoesNotClaimReservation(t *testing.T) {
	r, a := assumeStepFixture()
	node := r.resources.Snapshot().Nodes["node-a"].Node().DeepCopy()
	node.Status.Allocatable[corev1.ResourceCPU] = resource.MustParse("3")
	r.resources.UpdateNode(node)
	if err := r.Assume(context.Background(), a); !errors.Is(err, resourcecache.ErrAssumeNoFit) {
		t.Fatalf("最新资源账放不下时应带出可识别的错误: %v", err)
	}
	if a.Reserved || r.resources.Snapshot().Nodes["node-a"].GetRequested().GetMilliCPU() != 0 {
		t.Fatal("批量 Assume 失败留下了部分占用或错误标记")
	}
	// 本批次没有提交占用，恢复不需要访问资源账，更不应该执行 Forget。
	r.resources = nil
	if err := r.Recover(context.Background(), a); err != nil {
		t.Fatalf("Assume 失败且未记账时应直接完成恢复: %v", err)
	}
}

func TestRecoverBeforeAssumeNeedsNoResourceCache(t *testing.T) {
	for _, mode := range []workqueue.SubmitMode{workqueue.AllTogether, workqueue.AllowPartial} {
		t.Run(string(mode), func(t *testing.T) {
			_, a := assumeStepFixture()
			a.Request.Mode = mode
			// 已有计划，但没有执行 Assume；两种模式都不需要退账。
			r := NewRunner(nil, nil)
			if err := r.Recover(context.Background(), a); err != nil {
				t.Fatalf("规划阶段失败不应要求 Forget: %v", err)
			}
		})
	}
}

func TestRecoverOnlyForgetsRecordedMembers(t *testing.T) {
	r, a := assumeStepFixture()
	a.Request.Mode = workqueue.AllowPartial
	// 模拟后续 Partial 执行的边界：原请求含 A、B，但本轮只有 A 暂占成功。
	// B 的占用属于其他执行；恢复本轮时不能拿原请求的全部成员去退账。
	for i, pod := range a.members {
		copy := pod.DeepCopy()
		copy.Spec.NodeName = "node-a"
		owner, action := a.Request.Unit, a.Request.ActionID
		if i == 1 {
			owner.ID, action = "other-unit", "other-action"
		}
		if err := r.resources.Assume([]resourcecache.Assumption{{Unit: owner, ActionID: action, Pod: copy}}); err != nil {
			t.Fatal(err)
		}
	}
	a.Reserved = true
	a.AssumedMembers = []workqueue.PodRef{a.Request.Members[0]}
	if err := r.Recover(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	if a.Reserved || len(a.AssumedMembers) != 0 || r.resources.Snapshot().Nodes["node-a"].GetRequested().GetMilliCPU() != 2000 {
		t.Fatal("恢复没有只撤销本轮暂占，或误动了其他执行的占用")
	}
}

func TestRecoverKeepsReservationWhenBindingIsUnknown(t *testing.T) {
	r, a := assumeStepFixture()
	if err := r.Assume(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	a.Binding = true
	if err := r.Recover(context.Background(), a); !errors.Is(err, ErrBindingResultUnknown) {
		t.Fatalf("绑定结果尚未确认时不能直接退账: %v", err)
	}
	if !a.Reserved || r.resources.Snapshot().Nodes["node-a"].GetRequested().GetMilliCPU() != 4000 {
		t.Fatal("未知绑定被当作明确失败，提前释放了暂占")
	}
}

func TestNewRecoveryRequestCannotForgetEarlierExecution(t *testing.T) {
	r, a := assumeStepFixture()
	if err := r.Assume(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	a.Request.Operation, a.Request.ActionID = staging.OperationRecover, "new-recovery"
	if err := r.Recover(context.Background(), a); !errors.Is(err, staging.ErrHandlerUnavailable) {
		t.Fatalf("新的恢复请求还不能直接撤销旧执行: %v", err)
	}
	if !a.Reserved || r.resources.Snapshot().Nodes["node-a"].GetRequested().GetMilliCPU() != 4000 {
		t.Fatal("尚未确认旧执行归属就释放了资源")
	}
}
