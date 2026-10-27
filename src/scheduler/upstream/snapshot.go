package upstream

import (
	"fmt"
	"sort"

	corev1 "k8s.io/api/core/v1"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// SnapshotNodeInfos 导出 Filter/Score 使用的这一轮节点信息，不再读取 informer。
// 原生 Snapshot 会复制资源统计、端口和成员列表，后续试放不会改掉过滤器的资源账。
// 列表中的已有 Pod 和 Node 对象仍按原生约定共享，只读；要修改对象本身时先 DeepCopy。
func (f *PodFilter) SnapshotNodeInfos() map[string]fwk.NodeInfo {
	nodes := make(map[string]fwk.NodeInfo, len(f.nodes))
	for _, node := range f.nodes {
		nodes[node.Node().Name] = node.Snapshot()
	}
	return nodes
}

// buildNodeInfos 把 Node 和已分配 Pod 放进原生 NodeInfo，由上游代码计算资源和端口占用。
// 先复制输入，不让本轮视图跟着 informer 共享对象变化；不回写传入的 Pod/Node。
func buildNodeInfos(nodes []*corev1.Node, pods []*corev1.Pod) ([]fwk.NodeInfo, error) {
	byName := make(map[string]*framework.NodeInfo, len(nodes))
	for _, node := range nodes {
		if node == nil || node.Name == "" || byName[node.Name] != nil {
			return nil, fmt.Errorf("节点视图包含空节点或重复名称")
		}
		info := framework.NewNodeInfo()
		info.SetNode(node.DeepCopy())
		byName[node.Name] = info
	}
	seen := make(map[string]bool)
	for _, pod := range pods {
		if pod == nil || pod.UID == "" {
			return nil, fmt.Errorf("节点视图中的 Pod 缺少 UID")
		}
		if seen[string(pod.UID)] {
			return nil, fmt.Errorf("节点视图重复计算 Pod: %s/%s", pod.Namespace, pod.Name)
		}
		seen[string(pod.UID)] = true
		if info := byName[pod.Spec.NodeName]; info != nil {
			if err := checkExistingPodConstraints(pod); err != nil {
				return nil, err
			}
			info.AddPod(pod.DeepCopy())
		}
		// 未绑定 Pod 不占节点；节点已不存在的 Pod 也不属于本轮候选节点。
	}
	names := make([]string, 0, len(byName))
	for name := range byName {
		names = append(names, name)
	}
	sort.Strings(names)
	infos := make([]fwk.NodeInfo, 0, len(names))
	for _, name := range names {
		infos = append(infos, byName[name])
	}
	return infos, nil
}

// checkExistingPodConstraints 保留已有 Pod 对新成员的限制，两个构造入口都必须检查。
// 当前未接入 InterPodAffinity，不能把这些 Pod 的硬反亲和性忽略掉。
func checkExistingPodConstraints(pod *corev1.Pod) error {
	if a := pod.Spec.Affinity; a != nil && a.PodAntiAffinity != nil && len(a.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution) != 0 {
		return fmt.Errorf("尚未接入原生 InterPodAffinity，节点视图含硬反亲和性 Pod: %s/%s", pod.Namespace, pod.Name)
	}
	return nil
}

// checkSupportedConstraints 防止把“尚未接入的硬约束”当成“没有约束”。
// 返回错误让调用方明确知道本次无法完成过滤，不把不完整检查当作可部署结论。
func checkSupportedConstraints(pod *corev1.Pod) error {
	if len(pod.Spec.SchedulingGates) != 0 {
		return fmt.Errorf("Pod 的 schedulingGates 尚未解除，不能开始调度过滤")
	}
	if affinity := pod.Spec.Affinity; affinity != nil {
		if affinity.PodAffinity != nil && len(affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution) != 0 {
			return fmt.Errorf("尚未接入原生 InterPodAffinity，不能检查 Pod 间硬亲和性")
		}
		if affinity.PodAntiAffinity != nil && len(affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution) != 0 {
			return fmt.Errorf("尚未接入原生 InterPodAffinity，不能检查 Pod 间硬反亲和性")
		}
	}
	for _, constraint := range pod.Spec.TopologySpreadConstraints {
		if constraint.WhenUnsatisfiable == corev1.DoNotSchedule {
			return fmt.Errorf("尚未接入原生 PodTopologySpread，不能检查硬拓扑分布约束")
		}
	}
	for _, volume := range pod.Spec.Volumes {
		// 这些卷不需要本轮节点存储过滤；其他卷先拒绝，后面接 Volume 系列插件。
		if volume.EmptyDir == nil && volume.Secret == nil && volume.ConfigMap == nil && volume.DownwardAPI == nil && volume.Projected == nil && volume.HostPath == nil && volume.Image == nil {
			return fmt.Errorf("尚未接入原生存储过滤，不能检查卷 %s", volume.Name)
		}
	}
	if len(pod.Spec.ResourceClaims) != 0 {
		return fmt.Errorf("尚未接入原生 DynamicResources，不能检查资源声明")
	}
	for _, container := range append(append([]corev1.Container(nil), pod.Spec.InitContainers...), pod.Spec.Containers...) {
		if len(container.Resources.Claims) != 0 {
			return fmt.Errorf("尚未接入原生 DynamicResources，不能检查容器资源声明")
		}
	}
	return nil
}
