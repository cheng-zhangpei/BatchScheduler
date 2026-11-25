package resourcecache

import (
	"testing"
	"time"

	"batchscheduler/src/workqueue"
)

func TestBindingCheckKeepsOriginalDeadlineAndResources(t *testing.T) {
	c := New()
	c.UpdateNode(observedNode())
	batch := assumptionBatch()
	if err := c.Assume(batch); err != nil {
		t.Fatal(err)
	}
	plan := make(map[workqueue.PodRef]string)
	for _, assumption := range batch {
		plan[workqueue.PodRef{Name: assumption.Pod.Name, UID: assumption.Pod.UID}] = assumption.Pod.Spec.NodeName
	}
	start := time.Unix(100, 0)
	owner := batch[0]
	deadline := start.Add(30 * time.Second)
	if err := c.TrackBindingChecks(owner.Unit, owner.ActionID, plan, deadline); err != nil {
		t.Fatal(err)
	}
	key := BindingCheckKey{Unit: owner.Unit, ActionID: owner.ActionID, Pod: workqueue.PodRef{Name: owner.Pod.Name, UID: owner.Pod.UID}}
	if err := c.TrackBindingChecks(owner.Unit, owner.ActionID, plan, start.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	check := c.bindingChecks[key]
	if !check.Deadline.Equal(deadline) || check.ReadyToRelease(start.Add(29*time.Second)) || !check.ReadyToRelease(deadline) {
		t.Fatalf("查询或重复登记重置了原截止时间: %+v", check)
	}
	if c.Snapshot().Nodes["node-a"].GetRequested().GetMilliCPU() != 4000 {
		t.Fatal("查询过程提前改变了暂占")
	}
	if err := c.UpdatePod(owner.Pod); err != nil {
		t.Fatal(err)
	}
	unbound := owner.Pod.DeepCopy()
	unbound.Spec.NodeName = ""
	if err := c.UpdatePod(unbound); err != nil {
		t.Fatal(err)
	}
	if len(c.Snapshot().BoundPods) != 1 || c.Snapshot().Nodes["node-a"].GetRequested().GetMilliCPU() != 4000 {
		t.Fatal("真实绑定被重复记账，或被旧未绑定查询覆盖")
	}
	c.CompleteBindingChecks([]BindingCheckKey{key})
	if len(c.BindingChecks()) != 1 || len(c.assumed) != 1 {
		t.Fatal("结束核查改变了另一成员的记录或暂占")
	}
}

func TestBindingCheckDeletionCanSettleBeforeDeadline(t *testing.T) {
	c := New()
	c.UpdateNode(observedNode())
	pod := observedPod()
	owner := assumptionFor(pod)
	if err := c.Assume([]Assumption{owner}); err != nil {
		t.Fatal(err)
	}
	ref := workqueue.PodRef{Name: pod.Name, UID: pod.UID}
	if err := c.TrackBindingChecks(owner.Unit, owner.ActionID, map[workqueue.PodRef]string{ref: pod.Spec.NodeName}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := c.RemovePod(pod); err != nil {
		t.Fatal(err)
	}
	key := BindingCheckKey{Unit: owner.Unit, ActionID: owner.ActionID, Pod: ref}
	result, err := c.ReclaimBindingChecks([]BindingCheckKey{key}, time.Now())
	if err != nil || !result.Ready || len(result.Released) != 1 {
		t.Fatalf("没有结束原对象的等待: %+v, %v", result, err)
	}
	if c.Snapshot().Nodes["node-a"].GetRequested().GetMilliCPU() != 0 {
		t.Fatal("旧 GET 恢复了已经删除的 Pod 占用")
	}
}
