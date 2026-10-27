package resourcecache

import (
	"reflect"
	"testing"

	"batchscheduler/src/group"
	"batchscheduler/src/workqueue"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func assumptionFor(pod *corev1.Pod) Assumption {
	return Assumption{Unit: workqueue.UnitKey{Group: group.Key{Namespace: pod.Namespace, Name: "job", UID: "group"}, ID: "unit"},
		ActionID: "action", Pod: pod}
}

func assumptionBatch() []Assumption {
	a, b := observedPod(), observedPod()
	a.Name, a.UID, b.Name, b.UID = "a", "a", "b", "b"
	a.Spec.Containers[0].Ports, b.Spec.Containers[0].Ports = nil, nil
	return []Assumption{assumptionFor(a), assumptionFor(b)}
}

func TestAssumeBatchPublishesOnceAndKeepsIndependentPods(t *testing.T) {
	c := New()
	c.UpdateNode(observedNode())
	before := c.Snapshot()
	batch := assumptionBatch()
	for range 2 {
		if err := c.Assume(batch); err != nil {
			t.Fatal(err)
		}
	}
	view := c.Snapshot()
	if view.Nodes["node-a"].GetRequested().GetMilliCPU() != 4000 || len(view.Nodes["node-a"].GetPods()) != 2 ||
		len(view.BoundPods) != 0 || len(c.assumed) != 2 || len(c.pods) != 2 {
		t.Fatal("批量暂占没有同时写入三份账，重复登记多扣资源，或被当成真实绑定")
	}
	if before.Nodes["node-a"].GetRequested().GetMilliCPU() != 0 {
		t.Fatal("Assume 改变了已经取出的旧快照")
	}
	batch[0].Pod.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU] = resource.MustParse("3")
	if c.assumed["a"].Pod.Spec.Containers[0].Resources.Requests.Cpu().MilliValue() != 2000 {
		t.Fatal("暂占记录共享了调用方的可变 Pod")
	}
	if err := c.Assume(batch); err == nil {
		t.Fatal("相同执行编号不能改变已经登记的资源需求")
	}
	if c.Snapshot().Nodes["node-a"].GetRequested().GetMilliCPU() != 4000 {
		t.Fatal("拒绝修改旧计划时改变了已有占用")
	}
}

func TestAssumeBatchFailureDoesNotLeavePartialAccounting(t *testing.T) {
	for _, name := range []string{"cpu", "host-port", "missing-node", "already-bound", "other-execution", "duplicate-uid"} {
		t.Run(name, func(t *testing.T) {
			c := New()
			node, batch := observedNode(), assumptionBatch()
			switch name {
			case "cpu":
				node.Status.Allocatable[corev1.ResourceCPU] = resource.MustParse("3")
			case "host-port":
				for _, record := range batch {
					record.Pod.Spec.Containers[0].Ports = []corev1.ContainerPort{{HostPort: 8080, Protocol: corev1.ProtocolTCP}}
				}
			case "missing-node":
				batch[1].Pod.Spec.NodeName = "gone"
			case "duplicate-uid":
				batch[1].Pod.UID = batch[0].Pod.UID
			}
			c.UpdateNode(node)
			if name == "already-bound" {
				if err := c.UpdatePod(batch[1].Pod); err != nil {
					t.Fatal(err)
				}
			}
			if name == "other-execution" {
				other := batch[1]
				other.ActionID = "earlier-action"
				if err := c.Assume([]Assumption{other}); err != nil {
					t.Fatal(err)
				}
			}
			before := c.Snapshot()
			pods, assumed := len(c.pods), len(c.assumed)
			if err := c.Assume(batch); err == nil {
				t.Fatal("无效或放不下的批次没有失败")
			}
			if !reflect.DeepEqual(c.Snapshot(), before) || len(c.pods) != pods || len(c.assumed) != assumed {
				t.Fatal("后面的成员失败时，前面的试放仍然改变了共享资源账")
			}
		})
	}
}

func TestConcurrentAssumeBatchesCannotSpendSameResources(t *testing.T) {
	c := New()
	c.UpdateNode(observedNode())
	start, results := make(chan struct{}), make(chan error, 2)
	for _, record := range assumptionBatch() {
		record.Pod.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU] = resource.MustParse("3")
		go func(record Assumption) {
			<-start
			results <- c.Assume([]Assumption{record})
		}(record)
	}
	close(start)
	successes := 0
	for range 2 {
		if err := <-results; err == nil {
			successes++
		}
	}
	if successes != 1 || c.Snapshot().Nodes["node-a"].GetRequested().GetMilliCPU() != 3000 || len(c.assumed) != 1 {
		t.Fatal("两个批次重复消费了同一份节点资源")
	}
}
