package upstream

import (
	corev1 "k8s.io/api/core/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nodeports"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/noderesources"
)

// FitsForPlacement 在当前方案的 NodeInfo 上调用两个上游检查：
// NodeResourcesFit 计算再放一个 Pod 后的资源与 Pod 数量，NodePorts 检查 HostPort。
// 两项都通过才返回 true，随后 trial.place 才会 AddPodInfo，把这次占用记入副本。
// 初始 NodeList 已由完整的已接入 Pod Filter 生成，这里只重算试放中会变化的资源和端口。
func FitsForPlacement(pod *corev1.Pod, node fwk.NodeInfo) bool {
	features := feature.NewSchedulerFeaturesFromGates(utilfeature.DefaultFeatureGate)
	resources := noderesources.Fits(pod, node, noderesources.ResourceRequestsOptions{
		EnablePodLevelResources: features.EnablePodLevelResources,
	})
	return len(resources) == 0 && nodeports.Fits(pod, node)
}
