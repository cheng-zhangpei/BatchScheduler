package upstream_test

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"batchscheduler/src/scheduler/steps"
	"batchscheduler/src/scheduler/upstream"
	"batchscheduler/src/workqueue"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func filterNode(name string) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}, Status: corev1.NodeStatus{
		Allocatable: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4"), corev1.ResourceMemory: resource.MustParse("8Gi"),
			corev1.ResourcePods: resource.MustParse("32"), "example.com/gpu": resource.MustParse("1")},
	}}
}

func requestingPod(name, cpu string) *corev1.Pod {
	pod := filterPod(name)
	pod.Spec.Containers = []corev1.Container{{Name: "app", Resources: corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu)},
	}}}
	return pod
}

// 这些用例调用实际的上游插件，检查适配时是否接好了数据和 PreFilter，而非模拟插件结果。
func TestUpstreamFilterUsesNativeRules(t *testing.T) {
	for _, name := range []string{"available", "occupied-cpu", "init-overhead", "gpu", "pod-slots", "selector", "field-affinity", "conflicting-affinity", "taint", "tolerated-taint", "unschedulable", "node-name", "host-port"} {
		t.Run(name, func(t *testing.T) {
			nodes := []*corev1.Node{filterNode("node-a"), filterNode("node-b")}
			pod := requestingPod("target", "1")
			var assigned []*corev1.Pod
			want := []string{"node-a", "node-b"}
			plugin, rejectedNode := "", "node-a"
			switch name {
			case "occupied-cpu", "pod-slots":
				other := requestingPod("ordinary-workload", "4")
				other.Spec.NodeName = "node-a" // 无 PodGroup 的普通任务也占资源。
				assigned = []*corev1.Pod{other}
				want, plugin = []string{"node-b"}, "NodeResourcesFit"
				if name == "pod-slots" {
					nodes[0].Status.Allocatable[corev1.ResourcePods] = resource.MustParse("1")
				}
			case "init-overhead":
				pod.Spec.InitContainers = []corev1.Container{{Name: "init", Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("3")}}}}
				pod.Spec.Overhead = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")}
				want, plugin = nil, "NodeResourcesFit"
			case "gpu":
				pod.Spec.Containers[0].Resources.Requests["example.com/gpu"] = resource.MustParse("2")
				want, plugin = nil, "NodeResourcesFit"
			case "selector":
				nodes[0].Labels = map[string]string{"disk": "ssd"}
				pod.Spec.NodeSelector = map[string]string{"disk": "ssd"}
				want, plugin, rejectedNode = []string{"node-a"}, "NodeAffinity", "node-b"
			case "field-affinity", "conflicting-affinity":
				fields := []corev1.NodeSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{"node-b"}}}
				if name == "conflicting-affinity" {
					fields = append(fields, corev1.NodeSelectorRequirement{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{"node-a"}})
				}
				pod.Spec.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchFields: fields}},
				}}}
				want, plugin = []string{"node-b"}, "NodeAffinity"
				if name == "conflicting-affinity" {
					want = nil
				}
			case "taint", "tolerated-taint":
				nodes[0].Spec.Taints = []corev1.Taint{{Key: "dedicated", Value: "batch", Effect: corev1.TaintEffectNoSchedule}}
				if name == "tolerated-taint" {
					pod.Spec.Tolerations = []corev1.Toleration{{Key: "dedicated", Value: "batch", Operator: corev1.TolerationOpEqual, Effect: corev1.TaintEffectNoSchedule}}
				} else {
					want, plugin = []string{"node-b"}, "TaintToleration"
				}
			case "unschedulable":
				nodes[0].Spec.Unschedulable = true
				want, plugin = []string{"node-b"}, "NodeUnschedulable"
			case "node-name":
				pod.Spec.NodeName = "node-a"
				want, plugin, rejectedNode = []string{"node-a"}, "NodeName", "node-b"
			case "host-port":
				port := corev1.ContainerPort{HostPort: 8080, Protocol: corev1.ProtocolTCP}
				other := requestingPod("server", "1")
				other.Spec.NodeName, other.Spec.Containers[0].Ports = "node-a", []corev1.ContainerPort{port}
				assigned = []*corev1.Pod{other}
				pod.Spec.Containers[0].Ports = []corev1.ContainerPort{port}
				want, plugin = []string{"node-b"}, "NodePorts"
			}
			filter, err := upstream.NewPodFilter(context.Background(), nodes, assigned)
			if err != nil {
				t.Fatal(err)
			}
			result, err := filter.Filter(context.Background(), pod)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(result.Candidates, want) {
				t.Fatalf("候选节点 = %v，期望 %v；拒绝 = %+v", result.Candidates, want, result.Rejected)
			}
			if plugin != "" {
				rejection := result.Rejected[rejectedNode]
				if rejection.Plugin != plugin || len(rejection.Reasons) == 0 {
					t.Fatalf("没有保留原生拒绝原因: %+v", rejection)
				}
			}
		})
	}
}

func TestGroupFilterWithNativeFiltersHasSeparatePodState(t *testing.T) {
	nodes := []*corev1.Node{filterNode("node-a"), filterNode("node-b")}
	a, b := requestingPod("a", "1"), requestingPod("b", "1")
	a.Spec.NodeSelector = map[string]string{"disk": "ssd"}
	b.Spec.Containers[0].Ports = []corev1.ContainerPort{{HostPort: 80, Protocol: corev1.ProtocolTCP}}
	nodes[0].Labels = map[string]string{"disk": "ssd"}
	filter, err := upstream.NewPodFilter(context.Background(), nodes, nil)
	if err != nil {
		t.Fatal(err)
	}
	// 构建后原始 informer 输入发生变化，不应改变这轮视图。
	nodes[0].Labels["disk"] = "hdd"
	nodes[1].Status.Allocatable[corev1.ResourceCPU] = resource.MustParse("0")
	results, err := steps.NewGroupFilter(filter).Filter(context.Background(), []*corev1.Pod{a, b})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(results[workqueue.PodRef{Name: "a", UID: "a"}].Candidates, []string{"node-a"}) || !reflect.DeepEqual(results[workqueue.PodRef{Name: "b", UID: "b"}].Candidates, []string{"node-a", "node-b"}) {
		t.Fatalf("不同 Pod 的状态互相污染，或节点视图被外部修改: %+v", results)
	}
}

func TestUpstreamFilterRejectsUnsupportedConstraints(t *testing.T) {
	for _, name := range []string{"affinity", "anti-affinity", "spread", "pvc", "dra", "gate"} {
		t.Run(name, func(t *testing.T) {
			pod := requestingPod("target", "1")
			keyword := "尚未接入"
			switch name {
			case "affinity":
				pod.Spec.Affinity = &corev1.Affinity{PodAffinity: &corev1.PodAffinity{RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{TopologyKey: "zone"}}}}
			case "anti-affinity":
				pod.Spec.Affinity = &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{TopologyKey: "zone"}}}}
			case "spread":
				pod.Spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{{TopologyKey: "zone", WhenUnsatisfiable: corev1.DoNotSchedule}}
			case "pvc":
				pod.Spec.Volumes = []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data"}}}}
			case "dra":
				pod.Spec.ResourceClaims = []corev1.PodResourceClaim{{Name: "device"}}
			case "gate":
				pod.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: "ready"}}
				keyword = "尚未解除"
			}
			filter, err := upstream.NewPodFilter(context.Background(), []*corev1.Node{filterNode("node-a")}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := filter.Filter(context.Background(), pod); err == nil || !strings.Contains(err.Error(), keyword) {
				t.Fatalf("未实现的约束被忽略: %v", err)
			}
		})
	}
}

func TestUpstreamSnapshotChecksExistingPodAntiAffinity(t *testing.T) {
	other := requestingPod("existing", "1")
	other.Spec.NodeName = "node-a"
	other.Spec.Affinity = &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{TopologyKey: "zone"}}}}
	if _, err := upstream.NewPodFilter(context.Background(), []*corev1.Node{filterNode("node-a")}, []*corev1.Pod{other}); err == nil {
		t.Fatal("既有 Pod 的硬反亲和性不能被忽略")
	}
}

func filterPod(name string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "work", Name: name, UID: types.UID(name)}}
}
