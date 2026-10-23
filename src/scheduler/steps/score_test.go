package steps

import (
	"context"
	"reflect"
	"testing"

	"batchscheduler/src/scheduler"
	"batchscheduler/src/scheduler/upstream"
	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestScorePassesModeAndEveryPodListToGroupInput(t *testing.T) {
	ctx := context.Background()
	pods := []*corev1.Pod{filterPod("a"), filterPod("b")}
	nodes := []*corev1.Node{{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}, Status: corev1.NodeStatus{Allocatable: corev1.ResourceList{
		corev1.ResourceCPU: resource.MustParse("4"), corev1.ResourceMemory: resource.MustParse("8Gi"), corev1.ResourcePods: resource.MustParse("32"),
	}}}}
	filter, err := upstream.NewPodFilter(ctx, nodes, nil)
	if err != nil {
		t.Fatal(err)
	}
	candidates, err := NewGroupFilter(filter).Filter(ctx, pods)
	if err != nil {
		t.Fatal(err)
	}
	refs := []workqueue.PodRef{{Name: "a", UID: "a"}, {Name: "b", UID: "b"}}
	r := &Runner{}
	var previous map[workqueue.PodRef][]scheduler.NodeScore
	for _, mode := range []workqueue.SubmitMode{workqueue.AllTogether, workqueue.AllowPartial} {
		a := &Attempt{Request: staging.Execution{Mode: mode, Members: refs}, members: pods, filter: filter}
		input, err := r.Score(ctx, a, candidates)
		if err != nil {
			t.Fatal(err)
		}
		if input.Mode != mode || len(input.PodScores) != len(refs) {
			t.Fatal("组评分输入丢失了模式或成员")
		}
		for _, ref := range refs {
			if len(input.PodScores[ref]) != 1 || input.PodScores[ref][0].Node != "node-a" {
				t.Fatal("没有把每个 Pod 的完整候选评分传给后续入口")
			}
		}
		if previous != nil && !reflect.DeepEqual(previous, input.PodScores) {
			t.Fatal("本轮 Pod 基础评分不应因 Atomic/Partial 不同而改变")
		}
		previous = input.PodScores
	}
}
