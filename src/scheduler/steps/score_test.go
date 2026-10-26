package steps

import (
	"context"
	"reflect"
	"testing"

	scheduling "batchscheduler/src/apis/scheduling/v1alpha1"
	"batchscheduler/src/group"
	"batchscheduler/src/scheduler"
	"batchscheduler/src/scheduler/upstream"
	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

func TestScorePassesModeAndEveryPodListToGroupInput(t *testing.T) {
	ctx := context.Background()
	pods := []*corev1.Pod{filterPod("a"), filterPod("b")}
	for _, pod := range pods {
		pod.Spec.Containers = []corev1.Container{{Name: "app", Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}}}}
	}
	nodes := []*corev1.Node{{ObjectMeta: metav1.ObjectMeta{Name: "node-a", Labels: map[string]string{"zone": "original"}}, Status: corev1.NodeStatus{Allocatable: corev1.ResourceList{
		corev1.ResourceCPU: resource.MustParse("4"), corev1.ResourceMemory: resource.MustParse("8Gi"), corev1.ResourcePods: resource.MustParse("32"),
	}}}}
	running := pods[0].DeepCopy()
	running.Name, running.UID, running.Spec.NodeName = "running", "running", "node-a"
	filter, err := upstream.NewPodFilter(ctx, nodes, []*corev1.Pod{running})
	if err != nil {
		t.Fatal(err)
	}
	// 模拟缓存继续变化；组规划仍必须拿到 Filter 固定的旧资源与拓扑。
	nodes[0].Status.Allocatable[corev1.ResourceCPU] = resource.MustParse("8")
	nodes[0].Labels["zone"] = "changed"
	candidates, err := NewGroupFilter(filter).Filter(ctx, pods)
	if err != nil {
		t.Fatal(err)
	}
	refs := []workqueue.PodRef{{Name: "a", UID: "a"}, {Name: "b", UID: "b"}}
	r := &Runner{}
	var previous map[workqueue.PodRef][]scheduler.NodeScore
	for _, mode := range []workqueue.SubmitMode{workqueue.AllTogether, workqueue.AllowPartial} {
		a := &Attempt{Request: staging.Execution{Mode: mode, Members: refs}, members: pods, filter: filter}
		a.Request.Definition = &group.Definition{Spec: scheduling.PodGroupSpec{Placement: &scheduling.PlacementSpec{
			Communication: []scheduling.CommunicationLink{{Name: "ab", From: "a", To: "b", Weight: 10}}, ResourceConsolidation: true,
		}}}
		input, err := r.Score(ctx, a, candidates)
		if err != nil {
			t.Fatal(err)
		}
		if input.Mode != mode || len(input.PodScores) != len(refs) || len(input.Pods) != len(refs) || len(input.NodeInfos) != 1 {
			t.Fatal("组评分输入丢失了模式、Pod、NodeInfo 或评分列表")
		}
		if input.Placement == nil || !input.Placement.ResourceConsolidation || input.Placement.Communication[0].Weight != 10 {
			t.Fatal("Score 没有把用户声明交给 Place 的输入")
		}
		input.Placement.Communication[0].Weight = 99
		if a.Request.Definition.Spec.Placement.Communication[0].Weight != 10 {
			t.Fatal("Place 的输入共享了执行请求中的声明")
		}
		for i, ref := range refs {
			if input.Pods[i].Name != ref.Name || input.Pods[i].UID != ref.UID || len(input.Pods[i].Spec.Containers) != 1 {
				t.Fatal("完整成员对象没有与评分列表对应")
			}
			if len(input.PodScores[ref]) != 1 || input.PodScores[ref][0].Node != "node-a" {
				t.Fatal("没有把每个 Pod 的完整候选评分传给后续入口")
			}
		}
		node := input.NodeInfos["node-a"]
		if node.Node().Labels["zone"] != "original" || node.GetAllocatable().GetMilliCPU() != 4000 || node.GetRequested().GetMilliCPU() != 1000 || len(node.GetPods()) != 1 {
			t.Fatal("没有保留本轮 NodeInfo 的拓扑、容量和已有占用")
		}
		input.Pods[0].Spec.Containers[0].Name = "changed"
		if a.members[0].Spec.Containers[0].Name != "app" {
			t.Fatal("组规划的成员对象修改影响了本轮原始输入")
		}
		trial, err := framework.NewPodInfo(pods[0].DeepCopy())
		if err != nil {
			t.Fatal(err)
		}
		node.AddPodInfo(trial)
		if node.GetRequested().GetMilliCPU() != 2000 || filter.SnapshotNodeInfos()["node-a"].GetRequested().GetMilliCPU() != 1000 {
			t.Fatal("组规划试放改动了 Filter/Score 的资源统计")
		}
		if previous != nil && !reflect.DeepEqual(previous, input.PodScores) {
			t.Fatal("本轮 Pod 基础评分不应因 Atomic/Partial 不同而改变")
		}
		previous = input.PodScores
	}
}
