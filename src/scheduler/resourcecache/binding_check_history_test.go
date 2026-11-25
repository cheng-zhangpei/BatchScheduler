package resourcecache

import (
	"testing"
	"time"

	"batchscheduler/src/workqueue"
	"k8s.io/apimachinery/pkg/types"
)

func releasedCheck(name string) BindingCheck {
	pod := observedPod()
	pod.Name, pod.UID = name, types.UID(name)
	assumption := assumptionFor(pod)
	return BindingCheck{
		Key: BindingCheckKey{Unit: assumption.Unit, ActionID: assumption.ActionID,
			Pod: workqueue.PodRef{Name: pod.Name, UID: pod.UID}},
		NodeName: pod.Spec.NodeName, State: BindingCheckReleased, ReleasedAt: time.Now(),
	}
}

func TestBindingCheckHistoryEvictsLeastRecentlyUsed(t *testing.T) {
	history := newBindingCheckHistory(2)
	a, b, c := releasedCheck("a"), releasedCheck("b"), releasedCheck("c")
	history.remember(a)
	history.remember(b)
	if _, ok := history.find(a.Key); !ok {
		t.Fatal("找不到刚保存的记录")
	}
	history.remember(c)
	if _, ok := history.find(b.Key); ok {
		t.Fatal("应淘汰最久未使用的 b，而不是刚查询过的 a")
	}
	if len(history.entries) != 2 || history.order.Len() != 2 {
		t.Fatal("历史超过容量，或索引和 LRU 链表数量不一致")
	}
	// 同一执行更新记录时不增加容量；取出的副本也不能修改内部记录。
	a.ReleasedAt = time.Unix(123, 0)
	history.remember(a)
	got, ok := history.find(a.Key)
	if !ok || !got.ReleasedAt.Equal(a.ReleasedAt) || len(history.entries) != 2 {
		t.Fatal("更新旧执行没有替换原记录")
	}
	got.NodeName = "changed"
	stored, _ := history.find(a.Key)
	if stored.NodeName != a.NodeName {
		t.Fatal("修改返回值改变了历史")
	}
	replacement := a.Key
	replacement.Pod.UID = "new-pod"
	if _, ok := history.find(replacement); ok {
		t.Fatal("同名新 Pod 命中了旧 Pod 的记录")
	}
	replacement = a.Key
	replacement.ActionID = "new-action"
	if _, ok := history.find(replacement); ok {
		t.Fatal("新执行命中了旧执行的记录")
	}
}

// 历史被淘汰后，晚到的绑定仍由正常 UpdatePod 记账，不依赖 LRU 命中。
func TestLateBindingIsAccountedAfterHistoryEviction(t *testing.T) {
	c := New()
	c.UpdateNode(observedNode())
	pod := observedPod()
	assumption := assumptionFor(pod)
	if err := c.Assume([]Assumption{assumption}); err != nil {
		t.Fatal(err)
	}
	ref := workqueue.PodRef{Name: pod.Name, UID: pod.UID}
	if err := c.Forget(assumption.Unit, assumption.ActionID, []workqueue.PodRef{ref}); err != nil {
		t.Fatal(err)
	}
	c.releasedChecks = newBindingCheckHistory(1)
	key := BindingCheckKey{Unit: assumption.Unit, ActionID: assumption.ActionID, Pod: ref}
	c.releasedChecks.remember(BindingCheck{Key: key, NodeName: pod.Spec.NodeName,
		State: BindingCheckReleased, ReleasedAt: time.Now()})
	c.releasedChecks.remember(releasedCheck("other"))
	if _, ok := c.releasedChecks.find(key); ok {
		t.Fatal("测试未制造历史淘汰")
	}
	if err := c.UpdatePod(pod); err != nil {
		t.Fatal(err)
	}
	snapshot := c.Snapshot()
	if snapshot.Nodes[pod.Spec.NodeName].GetRequested().GetMilliCPU() != 2000 || len(snapshot.BoundPods) != 1 {
		t.Fatal("历史淘汰后，实际绑定没有重新计入资源账")
	}
}
