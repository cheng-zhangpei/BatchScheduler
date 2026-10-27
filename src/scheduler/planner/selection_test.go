package planner

import (
	"errors"
	"testing"

	"batchscheduler/src/scheduler"
	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

func selectionInput() (scheduler.GroupScoreInput, workqueue.PodRef) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "a", UID: "a"}}
	return scheduler.GroupScoreInput{
		Pods: []*corev1.Pod{pod},
		NodeInfos: map[string]fwk.NodeInfo{
			"node-a": framework.NewNodeInfo(), "node-b": framework.NewNodeInfo(),
		},
	}, workqueue.PodRef{Name: pod.Name, UID: pod.UID}
}

func TestSelectBestUsesSearchOrderAndCopiesPlan(t *testing.T) {
	input, ref := selectionInput()
	plans := []CandidatePlan{
		{Placements: map[workqueue.PodRef]string{ref: "node-a"}, Scores: PlanScores{Pod: Score{Value: 10}}},
		{Placements: map[workqueue.PodRef]string{ref: "node-b"}, Scores: PlanScores{Pod: Score{Value: 100}}},
	}
	selected, err := selectBest(input, plans)
	if err != nil {
		t.Fatal(err)
	}
	if selected[ref] != "node-a" {
		t.Fatal("应沿用搜索的通信排序，不能按 Pod 分数重新选节点")
	}
	selected[ref] = "node-b"
	if plans[0].Placements[ref] != "node-a" {
		t.Fatal("执行器修改返回计划时影响了原候选")
	}
}

func TestSelectBestRejectsIncompleteSelectedPlan(t *testing.T) {
	input, ref := selectionInput()
	for _, tc := range []struct {
		name  string
		plans []CandidatePlan
	}{
		{name: "no-candidates"},
		{name: "missing-member", plans: []CandidatePlan{{Placements: map[workqueue.PodRef]string{}}}},
		{name: "wrong-uid", plans: []CandidatePlan{{Placements: map[workqueue.PodRef]string{{Name: "a", UID: "old"}: "node-a"}}}},
		{name: "empty-node", plans: []CandidatePlan{{Placements: map[workqueue.PodRef]string{ref: ""}}}},
		{name: "unknown-node", plans: []CandidatePlan{{Placements: map[workqueue.PodRef]string{ref: "missing"}}}},
		{name: "extra-member", plans: []CandidatePlan{{Placements: map[workqueue.PodRef]string{ref: "node-a", {Name: "b", UID: "b"}: "node-b"}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := selectBest(input, tc.plans); err == nil {
				t.Fatal("没有拒绝空候选或不完整的选中方案")
			}
		})
	}
}

func TestSelectBestDoesNotIgnoreAdditionalScore(t *testing.T) {
	input, ref := selectionInput()
	plans := []CandidatePlan{{Placements: map[workqueue.PodRef]string{ref: "node-a"},
		Scores: PlanScores{Consolidation: &Score{Value: 10}}}}
	if _, err := selectBest(input, plans); !errors.Is(err, staging.ErrHandlerUnavailable) {
		t.Fatalf("额外评分的取舍未定，不能忽略它直接继续执行: %v", err)
	}
}
