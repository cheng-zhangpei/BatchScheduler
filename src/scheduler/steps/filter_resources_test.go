package steps

import (
	"context"
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

func TestFilterReadsSharedResourcesAndKeepsCycleSnapshot(t *testing.T) {
	ctx := context.Background()
	key := group.Key{Namespace: "work", Name: "job", UID: "group"}
	pod := filterPod("target")
	pod.Spec.Containers = []corev1.Container{{Name: "app", Resources: corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")},
	}}}
	view := &group.View{Group: &scheduling.PodGroup{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name, UID: key.UID}},
		Members: []group.PodView{{Group: key, Pod: pod}}}
	resources := resourcecache.New()
	resources.UpdateNode(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}, Status: corev1.NodeStatus{Allocatable: corev1.ResourceList{
		corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourcePods: resource.MustParse("32"),
	}}})
	r := NewRunner(contextTestViews{view}, resources)
	request := staging.Execution{Unit: workqueue.UnitKey{Group: key, ID: "unit"}, Mode: workqueue.AllTogether,
		Members: []workqueue.PodRef{{Name: pod.Name, UID: pod.UID}}}
	first := &Attempt{Request: request}
	before, err := r.Filter(ctx, first)
	if err != nil || len(before[request.Members[0]].Candidates) != 1 {
		t.Fatalf("空闲节点没有成为候选: %v, %v", before, err)
	}
	// 只登记本地暂占，没有 informer 绑定通知，验证下一轮直接读共享账。
	occupier := pod.DeepCopy()
	occupier.Name, occupier.UID, occupier.Spec.NodeName = "occupier", "occupier", "node-a"
	occupier.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU] = resource.MustParse("2")
	if err := resources.Assume([]resourcecache.Assumption{{
		Unit: workqueue.UnitKey{Group: key, ID: "other-unit"}, ActionID: "other-action", Pod: occupier,
	}}); err != nil {
		t.Fatal(err)
	}
	scored, err := r.Score(ctx, first, before)
	if err != nil || scored.NodeInfos["node-a"].GetRequested().GetMilliCPU() != 0 {
		t.Fatalf("新占用改变了已经开始的本轮 Filter/Score 快照: %v", err)
	}
	after, err := r.Filter(ctx, &Attempt{Request: request})
	if err != nil {
		t.Fatal(err)
	}
	result := after[request.Members[0]]
	if len(result.Candidates) != 0 || result.Rejected["node-a"].Plugin != "NodeResourcesFit" {
		t.Fatal("下一轮没有看见共享资源账中的占用")
	}
	if err := resources.Forget(workqueue.UnitKey{Group: key, ID: "other-unit"}, "other-action",
		[]workqueue.PodRef{{Name: occupier.Name, UID: occupier.UID}}); err != nil {
		t.Fatal(err)
	}
	released, err := r.Filter(ctx, &Attempt{Request: request})
	if err != nil || len(released[request.Members[0]].Candidates) != 1 {
		t.Fatalf("共享账释放资源后，节点没有恢复为候选: %v", err)
	}
}
