package resourcecache

import (
	"testing"
	"time"

	"batchscheduler/src/workqueue"
)

func TestBindingCheckCountsSpacedObservationsAndKeepsResources(t *testing.T) {
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
	if err := c.TrackBindingChecks(owner.Unit, owner.ActionID, plan, start); err != nil {
		t.Fatal(err)
	}
	key := BindingCheckKey{Unit: owner.Unit, ActionID: owner.ActionID, Pod: workqueue.PodRef{Name: owner.Pod.Name, UID: owner.Pod.UID}}
	unbound := owner.Pod.DeepCopy()
	unbound.Spec.NodeName = ""
	for _, elapsed := range []time.Duration{500 * time.Millisecond, time.Second, time.Second} {
		if _, err := c.ObserveBindingCheck(key, unbound, start.Add(elapsed), time.Second); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.TrackBindingChecks(owner.Unit, owner.ActionID, plan, start.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	check := c.bindingChecks[key]
	if check.UnboundChecks != 1 || check.TimedOutAt != start {
		t.Fatalf("短间隔、重复查询或登记重置了计数: %+v", check)
	}
	policy := DefaultBindingCheckPolicy()
	policy.RequiredUnboundChecks = 1
	if !check.ReadyToRelease(policy) || c.Snapshot().Nodes["node-a"].GetRequested().GetMilliCPU() != 4000 {
		t.Fatal("阈值应只提出回收资格，不实际 Forget")
	}
	replacement := unbound.DeepCopy()
	replacement.UID = "replacement"
	if confirmed, err := c.ObserveBindingCheck(key, replacement, start.Add(3*time.Second), time.Second); err != nil || confirmed || c.bindingChecks[key].UnboundChecks != 1 {
		t.Fatal("同名新 Pod 被当作原成员")
	}
	if confirmed, err := c.ObserveBindingCheck(key, owner.Pod, start.Add(4*time.Second), time.Second); err != nil || !confirmed {
		t.Fatalf("没有接住真实绑定: %v", err)
	}
	if _, err := c.ObserveBindingCheck(key, unbound, start.Add(5*time.Second), time.Second); err != nil {
		t.Fatal(err)
	}
	if c.bindingChecks[key].UnboundChecks != 0 || len(c.Snapshot().BoundPods) != 1 || c.Snapshot().Nodes["node-a"].GetRequested().GetMilliCPU() != 4000 {
		t.Fatal("真实绑定被重复记账，或旧未绑定查询增加了计数")
	}
	c.CompleteBindingChecks([]BindingCheckKey{key})
	if len(c.BindingChecks()) != 1 || len(c.assumed) != 1 {
		t.Fatal("结束核查改变了另一成员的记录或暂占")
	}
}

func TestBindingCheckDoesNotRestoreDeletedPod(t *testing.T) {
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
	if confirmed, err := c.ObserveBindingCheck(key, pod, time.Now(), time.Second); err != nil || confirmed {
		t.Fatalf("旧 GET 覆盖了已经处理的删除: %v", err)
	}
	if c.Snapshot().Nodes["node-a"].GetRequested().GetMilliCPU() != 0 {
		t.Fatal("旧 GET 恢复了已经删除的 Pod 占用")
	}
}
