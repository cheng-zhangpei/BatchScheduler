package communication

import (
	"context"
	"errors"
	"fmt"
	"testing"

	scheduling "batchscheduler/src/apis/scheduling/v1alpha1"
	"batchscheduler/src/scheduler"
	"batchscheduler/src/scheduler/upstream"
	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func placementInput(t *testing.T, members, nodes int) scheduler.GroupScoreInput {
	t.Helper()
	input := scheduler.GroupScoreInput{Mode: workqueue.AllTogether, PodScores: make(map[workqueue.PodRef][]scheduler.NodeScore)}
	for i := 0; i < members; i++ {
		id := fmt.Sprintf("worker-%d", i)
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "work", Name: "pod-" + id, UID: types.UID(id), Labels: map[string]string{scheduling.MemberIDLabel: id}},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("1Gi"),
			}}}}}}
		input.Pods = append(input.Pods, pod)
	}
	var cluster []*corev1.Node
	for i := 0; i < nodes; i++ {
		cluster = append(cluster, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("node-%d", i), Labels: map[string]string{
			scheduling.GroupName + "/rack": "rack-0", corev1.LabelTopologyZone: "zone-0",
		}}, Status: corev1.NodeStatus{Allocatable: corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse("4"), corev1.ResourceMemory: resource.MustParse("8Gi"), corev1.ResourcePods: resource.MustParse("32"),
		}}})
	}
	filter, err := upstream.NewPodFilter(context.Background(), cluster, nil)
	if err != nil {
		t.Fatal(err)
	}
	input.NodeInfos = filter.SnapshotNodeInfos()
	for _, pod := range input.Pods {
		filtered, err := filter.Filter(context.Background(), pod)
		if err != nil {
			t.Fatal(err)
		}
		for i, name := range filtered.Candidates {
			input.PodScores[podRef(pod)] = append(input.PodScores[podRef(pod)], scheduler.NodeScore{Node: name, Score: int64(100 - i*10)})
		}
	}
	return input
}

func relation(name, from, to string, priority int32) scheduling.CommunicationLink {
	return scheduling.CommunicationLink{Name: name, From: from, To: to, PreferredLevel: scheduling.TopologyNode, Weight: 10, Priority: priority}
}

func TestPlaceKeepsDifferentLayoutsAndPodScores(t *testing.T) {
	input := placementInput(t, 2, 2)
	input.Placement = &scheduling.PlacementSpec{Communication: []scheduling.CommunicationLink{relation("ab", "worker-0", "worker-1", 100)}}
	// 两端对同一节点的基础分可以不同。
	input.PodScores[podRef(input.Pods[1])][0].Score = 70
	input.PodScores[podRef(input.Pods[1])][1].Score = 95
	plans, err := New().Place(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if len(plans) == 0 || len(plans) > 4 {
		t.Fatalf("候选数量不符合约定: %d", len(plans))
	}
	a, b := podRef(input.Pods[0]), podRef(input.Pods[1])
	concentrated, spread := false, false
	for _, plan := range plans {
		if plan.Placements[a] == plan.Placements[b] {
			concentrated = true
		} else {
			spread = true
		}
		var expected int64
		for ref, node := range plan.Placements {
			for _, score := range input.PodScores[ref] {
				if score.Node == node {
					expected += score.Score
				}
			}
		}
		if plan.Scores.Pod.Value != expected {
			t.Fatal("没有保留两端各自的 Pod 基础分")
		}
	}
	if !concentrated || !spread {
		t.Fatal("应该同时保留集中与分散的方案，供后面评分")
	}
	for _, node := range input.NodeInfos {
		if node.GetRequested().GetMilliCPU() != 0 || len(node.GetPods()) != 0 {
			t.Fatal("试放修改了输入 NodeInfo")
		}
	}
}

func TestPlaceAccumulatesSharedEndpointAndDegradesLowerPriority(t *testing.T) {
	input := placementInput(t, 3, 2)
	// 故意把低优先级写在前面，实际仍应先保护 ab。
	input.Placement = &scheduling.PlacementSpec{Communication: []scheduling.CommunicationLink{
		relation("ac", "worker-0", "worker-2", 10), relation("ab", "worker-0", "worker-1", 100),
	}}
	plans, err := New().Place(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	a, b, c := podRef(input.Pods[0]), podRef(input.Pods[1]), podRef(input.Pods[2])
	best := plans[0]
	if best.Placements[a] != best.Placements[b] || best.Placements[a] == best.Placements[c] {
		t.Fatalf("应该优先保留 ab 同节点、让 ac 降级: %+v", best.Placements)
	}
	for _, plan := range plans {
		if len(plan.Placements) != 3 || len(plan.Notes) != 2 {
			t.Fatal("共享端点导致漏成员或漏关系")
		}
		counts := make(map[string]int)
		for _, node := range plan.Placements {
			counts[node]++
		}
		for _, count := range counts {
			if count > 2 {
				t.Fatal("多个链路合并后超出了节点累计资源")
			}
		}
		var total int64
		for ref, node := range plan.Placements {
			for _, score := range input.PodScores[ref] {
				if score.Node == node {
					total += score.Score
				}
			}
		}
		if plan.Scores.Pod.Value != total {
			t.Fatal("共享端点被重复加分")
		}
	}
}

func TestPlaceEqualPriorityUsesNameAndKeepsAlternative(t *testing.T) {
	input := placementInput(t, 3, 2)
	input.Placement = &scheduling.PlacementSpec{Communication: []scheduling.CommunicationLink{
		relation("ac", "worker-0", "worker-2", 100), relation("ab", "worker-0", "worker-1", 100),
	}}
	plans, err := New().Place(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	a, b, c := podRef(input.Pods[0]), podRef(input.Pods[1]), podRef(input.Pods[2])
	if len(plans) == 0 || plans[0].Placements[a] != plans[0].Placements[b] {
		t.Fatal("同优先级应先保护名称靠前的 ab，不受声明顺序影响")
	}
	// 优先保护 ab 不等于禁止其他放法；后续评分仍应能看到让 ac 更近的候选。
	for _, plan := range plans {
		if plan.Placements[a] == plan.Placements[c] && plan.Placements[a] != plan.Placements[b] {
			return
		}
	}
	t.Fatal("没有保留 ac 同节点的另一种放法")
}

func TestPlaceFallsBackAcrossMissingRackAndConflictingPorts(t *testing.T) {
	input := placementInput(t, 2, 2)
	for _, pod := range input.Pods {
		pod.Spec.Containers[0].Ports = []corev1.ContainerPort{{HostPort: 8080, Protocol: corev1.ProtocolTCP}}
	}
	for _, node := range input.NodeInfos {
		delete(node.Node().Labels, scheduling.GroupName+"/rack")
	}
	input.Placement = &scheduling.PlacementSpec{Communication: []scheduling.CommunicationLink{relation("ab", "worker-0", "worker-1", 100)}}
	plans, err := New().Place(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	for _, plan := range plans {
		if plan.Placements[podRef(input.Pods[0])] == plan.Placements[podRef(input.Pods[1])] {
			t.Fatal("资源足够也不能忽略本轮新增 Pod 的 HostPort 冲突")
		}
		if plan.Scores.Communication.Value != 10 {
			t.Fatal("缺少机架标签时应该按同 Zone 评价，不应假定同 Rack")
		}
	}
}

func TestPlaceIncludesUnlinkedMembersAndReportsSearchFailure(t *testing.T) {
	for _, nodes := range []int{1, 2} {
		t.Run(fmt.Sprintf("nodes-%d", nodes), func(t *testing.T) {
			input := placementInput(t, 3, nodes)
			input.Placement = &scheduling.PlacementSpec{Communication: []scheduling.CommunicationLink{relation("ab", "worker-0", "worker-1", 100)}}
			plans, err := New().Place(context.Background(), input)
			if nodes == 1 {
				if err == nil || errors.Is(err, staging.ErrHandlerUnavailable) {
					t.Fatal("累计资源不足应返回实际搜索失败，不是开发未完成")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, plan := range plans {
				if len(plan.Placements) != 3 {
					t.Fatal("没有补齐未参与通信关系的成员")
				}
			}
		})
	}
	// 没有通信声明时也应能形成普通组方案。
	if plans, err := New().Place(context.Background(), placementInput(t, 2, 2)); err != nil || len(plans) == 0 {
		t.Fatalf("无通信偏好不能阻止普通放置: %v", err)
	}
}
