package scheduler

import (
	"context"

	corev1 "k8s.io/api/core/v1"
)

// PodFilter 完成一个 Pod 的候选节点过滤，包括需要的 PreFilter 准备工作。
// upstream 包调用原生插件，steps 包把单 Pod 过滤组合成组过滤。
// 同一轮组过滤应使用同一份节点和资源视图，由 PodFilter 的实现持有。
type PodFilter interface {
	Filter(context.Context, *corev1.Pod) (PodFilterResult, error)
}

// NodeRejection 说明一个节点为什么没有通过，供后续映射内部错误和排查使用。
type NodeRejection struct {
	Plugin       string
	Reasons      []string
	Unresolvable bool // 原生过滤认为仅释放节点上的其他 Pod 也解决不了此限制。
}

// PodFilterResult 保留单个 Pod 的过滤结果，不代表已经选择或占用了节点。
// Candidates 为空也是正常结果，表示本轮没找到可放置节点，不等于过滤程序出错。
type PodFilterResult struct {
	Candidates []string                 // 通过过滤的节点名称。
	Rejected   map[string]NodeRejection // 未通过的节点及原因。
}
