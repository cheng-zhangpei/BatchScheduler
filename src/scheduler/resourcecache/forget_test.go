package resourcecache

import (
	"reflect"
	"testing"

	"batchscheduler/src/workqueue"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func assumptionRefs(batch []Assumption) []workqueue.PodRef {
	refs := make([]workqueue.PodRef, 0, len(batch))
	for _, record := range batch {
		refs = append(refs, workqueue.PodRef{Name: record.Pod.Name, UID: record.Pod.UID})
	}
	return refs
}

func TestForgetReleasesRequestedMembersAndCanRepeat(t *testing.T) {
	c := New()
	c.UpdateNode(observedNode())
	batch := assumptionBatch()
	batch[0].Pod.Spec.Containers[0].Ports = []corev1.ContainerPort{{HostPort: 8080, Protocol: corev1.ProtocolTCP}}
	if err := c.Assume(batch); err != nil {
		t.Fatal(err)
	}
	refs, before := assumptionRefs(batch), c.Snapshot()
	// 调用方原对象已经改变，Forget 仍按缓存中的原节点和原资源量退账。
	batch[0].Pod.Spec.NodeName = "changed"
	batch[0].Pod.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU] = resource.MustParse("100")
	for range 2 {
		if err := c.Forget(batch[0].Unit, batch[0].ActionID, refs[:1]); err != nil {
			t.Fatal(err)
		}
	}
	info := c.Snapshot().Nodes["node-a"]
	if info.GetRequested().GetMilliCPU() != 2000 || info.GetUsedPorts().CheckConflict("", "TCP", 8080) ||
		len(c.assumed) != 1 || len(c.pods) != 1 || c.assumed[refs[1].UID].Pod == nil {
		t.Fatal("没有释放指定成员的原占用，或重复退账影响了其他成员")
	}
	if before.Nodes["node-a"].GetRequested().GetMilliCPU() != 4000 {
		t.Fatal("Forget 改变了此前已经取出的快照")
	}
	if err := c.Forget(batch[1].Unit, batch[1].ActionID, []workqueue.PodRef{refs[1], refs[1]}); err != nil {
		t.Fatal(err)
	}
	if c.Snapshot().Nodes["node-a"].GetRequested().GetMilliCPU() != 0 || len(c.pods) != 0 || len(c.assumed) != 0 {
		t.Fatal("剩余成员没有清干净，或重复成员导致多退资源")
	}
}

func TestForgetKeepsConfirmedBinding(t *testing.T) {
	c := New()
	c.UpdateNode(observedNode())
	batch := assumptionBatch()
	if err := c.Assume(batch); err != nil {
		t.Fatal(err)
	}
	if err := c.UpdatePod(batch[0].Pod); err != nil {
		t.Fatal(err)
	}
	if err := c.Forget(batch[0].Unit, batch[0].ActionID, assumptionRefs(batch)); err != nil {
		t.Fatal(err)
	}
	view := c.Snapshot()
	if view.Nodes["node-a"].GetRequested().GetMilliCPU() != 2000 || len(view.BoundPods) != 1 ||
		view.BoundPods[0].UID != batch[0].Pod.UID || len(c.assumed) != 0 || len(c.pods) != 1 {
		t.Fatal("Forget 删除了已经确认绑定的实际占用")
	}
}

func TestForgetWrongOwnerDoesNotPartiallyRelease(t *testing.T) {
	for _, name := range []string{"other-action", "other-unit", "wrong-member-name"} {
		t.Run(name, func(t *testing.T) {
			c := New()
			c.UpdateNode(observedNode())
			batch := assumptionBatch()
			refs := assumptionRefs(batch)
			if name == "other-action" {
				batch[1].ActionID = "other-action"
			} else if name == "other-unit" {
				batch[1].Unit.ID = "other-unit"
			} else {
				refs[1].Name = "wrong-name"
			}
			for _, record := range batch {
				if err := c.Assume([]Assumption{record}); err != nil {
					t.Fatal(err)
				}
			}
			before := c.Snapshot()
			if err := c.Forget(batch[0].Unit, batch[0].ActionID, refs); err == nil {
				t.Fatal("没有拒绝其他执行或错误成员身份")
			}
			if !reflect.DeepEqual(c.Snapshot(), before) || len(c.assumed) != 2 || len(c.pods) != 2 {
				t.Fatal("第二个成员归属错误时，仍释放了第一个成员")
			}
		})
	}
}

func TestForgetAfterNodeDeletionClearsRemainingAccounting(t *testing.T) {
	c := New()
	node, batch := observedNode(), assumptionBatch()
	c.UpdateNode(node)
	if err := c.Assume(batch); err != nil {
		t.Fatal(err)
	}
	c.RemoveNode(node)
	if err := c.Forget(batch[0].Unit, batch[0].ActionID, assumptionRefs(batch)); err != nil {
		t.Fatal(err)
	}
	if len(c.nodes) != 0 || len(c.pods) != 0 || len(c.assumed) != 0 {
		t.Fatal("节点已经删除时，遗留暂占没有清干净")
	}
}
