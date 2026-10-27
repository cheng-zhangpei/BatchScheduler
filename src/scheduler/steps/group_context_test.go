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

type contextTestViews struct{ view *group.View }

func (v contextTestViews) Get(string, string) (*group.View, error) { return v.view, nil }

type contextReaderFunc func(context.Context, group.Key) (staging.GroupContext, error)

func (f contextReaderFunc) GroupContext(ctx context.Context, key group.Key) (staging.GroupContext, error) {
	return f(ctx, key)
}

func TestGroupInputKeepsHistorySeparateFromCurrentBoundPeers(t *testing.T) {
	ctx := context.Background()
	key := group.Key{Namespace: "work", Name: "job", UID: "group"}
	current, bound, ended, outsider := filterPod("new"), filterPod("running"), filterPod("ended"), filterPod("outsider")
	controller := true
	view := &group.View{Group: &scheduling.PodGroup{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name, UID: key.UID}}}
	resources := resourcecache.New()
	for _, pod := range []*corev1.Pod{current, bound, ended, outsider} {
		if pod != outsider {
			pod.OwnerReferences = []metav1.OwnerReference{{APIVersion: scheduling.GroupVersion.String(), Kind: "PodGroup", Name: key.Name, UID: key.UID, Controller: &controller}}
			view.Members = append(view.Members, group.PodView{Group: key, Pod: pod})
		}
		if pod != current {
			pod.Spec.NodeName = "node-a"
		}
		if pod == ended {
			pod.Status.Phase = corev1.PodSucceeded
		}
		if err := resources.UpdatePod(pod); err != nil {
			t.Fatal(err)
		}
	}
	resources.UpdateNode(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}, Status: corev1.NodeStatus{Allocatable: corev1.ResourceList{
		corev1.ResourceCPU: resource.MustParse("4"), corev1.ResourceMemory: resource.MustParse("8Gi"), corev1.ResourcePods: resource.MustParse("32"),
	}}})
	r := NewRunner(contextTestViews{view}, resources)
	unit := workqueue.UnitKey{Group: key, ID: "current"}
	other := workqueue.UnitKey{Group: key, ID: "other"}
	r.Groups = contextReaderFunc(func(_ context.Context, query group.Key) (staging.GroupContext, error) {
		if query != key {
			t.Fatal("没有按完整组身份查询")
		}
		return staging.GroupContext{Group: key, InFlight: []staging.InFlightUnit{{Unit: unit}, {Unit: other}},
			Succeeded: []staging.SuccessfulRecord{{ID: "old-success", Unit: workqueue.UnitKey{Group: key, ID: "old"},
				Members: []staging.SuccessfulMember{{Pod: workqueue.PodRef{Name: "gone", UID: "gone"}, Observed: staging.ObservedPod{NodeName: "node-old"}}}}}}, nil
	})
	a := &Attempt{Request: staging.Execution{Unit: unit, Mode: workqueue.AllowPartial, Members: []workqueue.PodRef{{Name: current.Name, UID: current.UID}}}}
	candidates, err := r.Filter(ctx, a)
	if err != nil {
		t.Fatal(err)
	}
	input, err := r.Score(ctx, a, candidates)
	if err != nil {
		t.Fatal(err)
	}
	if len(input.Context.Succeeded) != 1 || len(input.Context.InFlight) != 1 || input.Context.InFlight[0].Unit != other {
		t.Fatal("缺少成功历史或其他轮次，或把自己当成另一轮")
	}
	if len(input.BoundPeers) != 1 || input.BoundPeers[0].UID != bound.UID {
		t.Fatal("历史位置、已结束成员或其他组成员被混入当前绑定锚点")
	}
	if len(input.NodeInfos["node-a"].GetPods()) != 3 {
		t.Fatal("读取历史不能重复增加 NodeInfo 的资源占用")
	}
}
