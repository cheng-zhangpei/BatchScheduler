package resourcecache

import (
	"testing"
	"time"

	"batchscheduler/src/workqueue"
)

func TestBindingReclaimForgetsOnceAndObservesLateBinding(t *testing.T) {
	c, batch, keys, start := reclaimFixture(t)
	for range 2 {
		result, err := c.ReclaimBindingChecks(keys, start.Add(30*time.Second))
		if err != nil || !result.Ready || len(result.Released) != 2 || len(result.Bound) != 0 {
			t.Fatalf("回收结果不正确: %+v, %v", result, err)
		}
		if len(c.assumed) != 0 || c.Snapshot().Nodes["node-a"].GetRequested().GetMilliCPU() != 0 || len(c.releasedChecks.entries) != 2 {
			t.Fatal("回收没有退账和保留历史，或重复处理导致资源错误")
		}
	}
	unbound := batch[0].Pod.DeepCopy()
	unbound.Spec.NodeName = ""
	if err := c.UpdatePod(unbound); err != nil {
		t.Fatal(err)
	}
	if len(c.releasedChecks.entries) != 2 {
		t.Fatal("未绑定通知提前移除了历史")
	}
	replacement := batch[0].Pod.DeepCopy()
	replacement.UID = "replacement"
	if err := c.UpdatePod(replacement); err != nil {
		t.Fatal(err)
	}
	if len(c.releasedChecks.entries) != 2 {
		t.Fatal("同名新 UID 移除了旧记录")
	}
	if err := c.RemovePod(replacement); err != nil {
		t.Fatal(err)
	}
	for _, assumption := range batch {
		for range 2 {
			if err := c.UpdatePod(assumption.Pod); err != nil {
				t.Fatal(err)
			}
		}
	}
	if !c.releasedChecks.empty() || len(c.releasedChecks.byPod) != 0 || c.Snapshot().Nodes["node-a"].GetRequested().GetMilliCPU() != 4000 {
		t.Fatal("晚到绑定没有补回占用、重复计账，或历史索引没清理")
	}
	c.CompleteBindingChecks(keys)
	if len(c.bindingCheckPods) != 0 {
		t.Fatal("核查结束后遗留 UID 索引")
	}
}

func TestBindingReclaimPreservesAcknowledgedBindingsWithoutInformer(t *testing.T) {
	c, batch, keys, start := reclaimFixture(t)
	// B 已收到 Bind 成功响应，但 informer 尚未回来，仍必须保留它的真实占用。
	if err := c.ConfirmBoundMembers(batch[1].Unit, batch[1].ActionID, []workqueue.PodRef{keys[1].Pod}); err != nil {
		t.Fatal(err)
	}
	result, err := c.ReclaimBindingChecks(keys, start.Add(30*time.Second))
	if err != nil || !result.Ready || len(result.Bound) != 1 || result.Bound[0] != keys[1].Pod || len(result.Released) != 1 {
		t.Fatalf("忽略了回收前到达的确认: %+v, %v", result, err)
	}
	if c.Snapshot().Nodes["node-a"].GetRequested().GetMilliCPU() != 2000 {
		t.Fatal("扣掉了已经真实绑定的成员")
	}
}

func TestBindingReclaimWaitsForDeadlineAndBoundsHistory(t *testing.T) {
	c, _, keys, start := reclaimFixture(t)
	result, err := c.ReclaimBindingChecks(keys, start.Add(29*time.Second))
	if err != nil || result.Ready || len(c.assumed) != 2 || !c.releasedChecks.empty() {
		t.Fatal("预算未到期就修改了资源")
	}
	c.SetBindingHistoryCapacity(1)
	if _, err := c.ReclaimBindingChecks(keys, start.Add(30*time.Second)); err != nil {
		t.Fatal(err)
	}
	if len(c.releasedChecks.entries) != 1 || len(c.releasedChecks.byPod) != 1 {
		t.Fatal("历史或 UID 索引超过容量")
	}
	c.SetBindingHistoryCapacity(0)
	if !c.releasedChecks.empty() || len(c.releasedChecks.byPod) != 0 {
		t.Fatal("缩容历史没有同步清理索引")
	}
}

func reclaimFixture(t *testing.T) (*Cache, []Assumption, []BindingCheckKey, time.Time) {
	t.Helper()
	c := New()
	c.UpdateNode(observedNode())
	batch := assumptionBatch()
	if err := c.Assume(batch); err != nil {
		t.Fatal(err)
	}
	plan := make(map[workqueue.PodRef]string)
	var keys []BindingCheckKey
	for _, a := range batch {
		ref := workqueue.PodRef{Name: a.Pod.Name, UID: a.Pod.UID}
		plan[ref] = a.Pod.Spec.NodeName
		keys = append(keys, BindingCheckKey{Unit: a.Unit, ActionID: a.ActionID, Pod: ref})
	}
	start := time.Unix(100, 0)
	if err := c.TrackBindingChecks(batch[0].Unit, batch[0].ActionID, plan, start.Add(30*time.Second)); err != nil {
		t.Fatal(err)
	}
	return c, batch, keys, start
}
