package resourcecache

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

func observedNode() *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a", UID: "node-old"},
		Status: corev1.NodeStatus{Allocatable: corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse("4"), corev1.ResourcePods: resource.MustParse("32"),
		}}}
}

func observedPod() *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "other", Name: "worker", UID: "pod"},
		Spec: corev1.PodSpec{NodeName: "node-a", SchedulerName: "default-scheduler", Containers: []corev1.Container{{
			Name: "app", Ports: []corev1.ContainerPort{{HostPort: 8080, Protocol: corev1.ProtocolTCP}},
			Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")}},
		}}}}
}

func TestObservedPodBeforeNodeAndRepeatedUpdates(t *testing.T) {
	c := New()
	pod := observedPod()
	if err := c.UpdatePod(pod); err != nil {
		t.Fatal(err)
	}
	if len(c.SnapshotNodeInfos()) != 0 {
		t.Fatal("只收到 Pod 时，不能把缺少节点信息的账作为可调度节点")
	}
	c.UpdateNode(observedNode())
	if err := c.UpdatePod(pod); err != nil {
		t.Fatal(err)
	}
	info := c.SnapshotNodeInfos()["node-a"]
	if info.GetRequested().GetMilliCPU() != 2000 || len(info.GetPods()) != 1 || !info.GetUsedPorts().CheckConflict("", "TCP", 8080) {
		t.Fatal("Node 后到或重复 Pod 通知导致占用错误")
	}
	// 修改传入对象不会改变已登记的账，只有下一次 UpdatePod 才生效。
	pod.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU] = resource.MustParse("3")
	pod.Spec.Containers[0].Ports[0].HostPort = 9090
	if c.SnapshotNodeInfos()["node-a"].GetRequested().GetMilliCPU() != 2000 {
		t.Fatal("资源账共享了 informer 的可变 Pod 对象")
	}
	if err := c.UpdatePod(pod); err != nil {
		t.Fatal(err)
	}
	info = c.SnapshotNodeInfos()["node-a"]
	if info.GetRequested().GetMilliCPU() != 3000 || info.GetUsedPorts().CheckConflict("", "TCP", 8080) || !info.GetUsedPorts().CheckConflict("", "TCP", 9090) {
		t.Fatal("更新没有撤销旧资源和端口，再登记新占用")
	}
	for range 2 {
		if err := c.RemovePod(pod); err != nil {
			t.Fatal(err)
		}
	}
	if c.SnapshotNodeInfos()["node-a"].GetRequested().GetMilliCPU() != 0 {
		t.Fatal("删除后仍有占用或重复扣减")
	}
}

func TestNodeUpdateAndDeletePreservePodAccounting(t *testing.T) {
	c := New()
	node, pod := observedNode(), observedPod()
	c.UpdateNode(node)
	if err := c.UpdatePod(pod); err != nil {
		t.Fatal(err)
	}
	node.Status.Allocatable[corev1.ResourceCPU] = resource.MustParse("8")
	c.UpdateNode(node)
	info := c.SnapshotNodeInfos()[node.Name]
	if info.GetAllocatable().GetMilliCPU() != 8000 || info.GetRequested().GetMilliCPU() != 2000 {
		t.Fatal("节点更新清掉了已有占用")
	}
	c.RemoveNode(node)
	if len(c.SnapshotNodeInfos()) != 0 {
		t.Fatal("删除的节点仍出现在可调度视图中")
	}
	replacement := node.DeepCopy()
	replacement.UID = "node-new"
	c.UpdateNode(replacement)
	c.RemoveNode(node)
	if c.SnapshotNodeInfos()[node.Name] == nil || c.SnapshotNodeInfos()[node.Name].GetRequested().GetMilliCPU() != 2000 {
		t.Fatal("旧节点删除误删新节点，或 Node 变化丢失尚未删除的 Pod 占用")
	}
	if err := c.RemovePod(pod); err != nil {
		t.Fatal(err)
	}
}

func TestUnboundPodAndSnapshotDoNotChangeAccounting(t *testing.T) {
	c := New()
	c.UpdateNode(observedNode())
	pod := observedPod()
	pod.Spec.NodeName = ""
	if err := c.UpdatePod(pod); err != nil {
		t.Fatal(err)
	}
	snapshot := c.SnapshotNodeInfos()
	info, err := framework.NewPodInfo(observedPod())
	if err != nil {
		t.Fatal(err)
	}
	snapshot["node-a"].AddPodInfo(info)
	if len(c.SnapshotNodeInfos()["node-a"].GetPods()) != 0 {
		t.Fatal("未绑定 Pod 或快照试放改变了共享资源账")
	}
}

func TestSnapshotCountsAssumedPodWithoutCallingItBound(t *testing.T) {
	c := New()
	c.UpdateNode(observedNode())
	bound := observedPod()
	if err := c.UpdatePod(bound); err != nil {
		t.Fatal(err)
	}
	assumed := bound.DeepCopy()
	assumed.Name, assumed.UID = "assumed", "assumed"
	assumed.Spec.Containers[0].Ports = nil
	if err := c.Assume([]Assumption{assumptionFor(assumed)}); err != nil {
		t.Fatal(err)
	}
	view := c.Snapshot()
	if view.Nodes["node-a"].GetRequested().GetMilliCPU() != 4000 || len(view.BoundPods) != 1 || view.BoundPods[0].UID != bound.UID {
		t.Fatal("暂占未计入资源，或被混入了真实绑定成员")
	}
	if err := c.UpdatePod(assumed); err != nil {
		t.Fatal(err)
	}
	confirmed := c.Snapshot()
	if confirmed.Nodes["node-a"].GetRequested().GetMilliCPU() != 4000 || len(confirmed.BoundPods) != 2 {
		t.Fatal("确认绑定后重复计入资源，或没有转为真实绑定成员")
	}
}
