package steps

import (
	"context"
	"errors"
	"testing"

	"batchscheduler/src/scheduler"
	"batchscheduler/src/workqueue"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

type podFilterFunc func(context.Context, *corev1.Pod) (scheduler.PodFilterResult, error)

func (f podFilterFunc) Filter(ctx context.Context, pod *corev1.Pod) (scheduler.PodFilterResult, error) {
	return f(ctx, pod)
}

func filterPod(name string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "work", Name: name, UID: types.UID(name)}}
}

func TestGroupFilterCollectsEveryMemberIncludingNoCandidates(t *testing.T) {
	pods := []*corev1.Pod{filterPod("a"), filterPod("b"), filterPod("c")}
	calls := 0
	f := NewGroupFilter(podFilterFunc(func(_ context.Context, pod *corev1.Pod) (scheduler.PodFilterResult, error) {
		calls++
		if pod.Name == "a" {
			return scheduler.PodFilterResult{Rejected: map[string]scheduler.NodeRejection{
				"node-a": {Plugin: "NodeResourcesFit", Reasons: []string{"Insufficient cpu"}},
			}}, nil
		}
		return scheduler.PodFilterResult{Candidates: []string{"node-a"}}, nil
	}))
	results, err := f.Filter(context.Background(), pods)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 3 || len(results) != 3 {
		t.Fatal("无候选节点不应该中断其他成员的过滤")
	}
	a := results[workqueue.PodRef{Name: "a", UID: "a"}]
	if len(a.Candidates) != 0 || a.Rejected["node-a"].Plugin != "NodeResourcesFit" || a.Rejected["node-a"].Reasons[0] != "Insufficient cpu" {
		t.Fatal("没有保留节点过滤失败原因")
	}
	for _, name := range []string{"b", "c"} {
		if results[workqueue.PodRef{Name: name, UID: types.UID(name)}].Candidates[0] != "node-a" {
			t.Fatal("过滤层不应该因为其他成员也能放到此节点就排除它")
		}
	}
}

func TestGroupFilterPropagatesExecutionError(t *testing.T) {
	want := errors.New("snapshot unavailable")
	calls := 0
	f := NewGroupFilter(podFilterFunc(func(_ context.Context, _ *corev1.Pod) (scheduler.PodFilterResult, error) {
		calls++
		return scheduler.PodFilterResult{}, want
	}))
	result, err := f.Filter(context.Background(), []*corev1.Pod{filterPod("a"), filterPod("b")})
	if !errors.Is(err, want) || calls != 1 || result != nil {
		t.Fatal("程序错误应立即返回，不能伪装成无可用节点")
	}
}

func TestGroupFilterChecksBatchBeforeCallingPodFilter(t *testing.T) {
	for _, name := range []string{"empty", "nil-member", "missing-uid", "duplicate", "different-namespace", "cancelled"} {
		t.Run(name, func(t *testing.T) {
			pods := []*corev1.Pod{filterPod("a"), filterPod("b")}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch name {
			case "empty":
				pods = nil
			case "nil-member":
				pods[1] = nil
			case "missing-uid":
				pods[1].UID = ""
			case "duplicate":
				pods[1] = pods[0]
			case "different-namespace":
				pods[1].Namespace = "other"
			case "cancelled":
				cancel()
			}
			calls := 0
			f := NewGroupFilter(podFilterFunc(func(context.Context, *corev1.Pod) (scheduler.PodFilterResult, error) {
				calls++
				return scheduler.PodFilterResult{}, nil
			}))
			if _, err := f.Filter(ctx, pods); err == nil || calls != 0 {
				t.Fatal("无效批次不应执行一半才被发现")
			}
		})
	}
	if _, err := NewGroupFilter(nil).Filter(context.Background(), []*corev1.Pod{filterPod("a")}); err == nil {
		t.Fatal("缺少单 Pod 实现时应明确报错")
	}
}
