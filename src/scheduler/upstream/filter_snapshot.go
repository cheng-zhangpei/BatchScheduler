package upstream

import (
	"context"
	"fmt"
	"sort"

	fwk "k8s.io/kube-scheduler/framework"
)

// NewPodFilterFromSnapshot 直接使用 ResourceCache 已统计好的 NodeInfo，不从 lister 重算占用。
// 先固定节点顺序并复制资源统计，再接入原来的六个插件；后续 Score 继续使用这份视图。
func NewPodFilterFromSnapshot(ctx context.Context, nodes map[string]fwk.NodeInfo) (*PodFilter, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	names := make([]string, 0, len(nodes))
	for name := range nodes {
		names = append(names, name)
	}
	sort.Strings(names)
	infos := make([]fwk.NodeInfo, 0, len(names))
	for _, name := range names {
		info := nodes[name]
		if info == nil || info.Node() == nil || info.Node().Name != name {
			return nil, fmt.Errorf("资源快照缺少节点信息或名称不一致: %s", name)
		}
		for _, pod := range info.GetPods() {
			if err := checkExistingPodConstraints(pod.GetPod()); err != nil {
				return nil, err
			}
		}
		infos = append(infos, info.Snapshot())
	}
	plugins, err := newUpstreamFilterPlugins(ctx)
	if err != nil {
		return nil, err
	}
	return &PodFilter{nodes: infos, plugins: plugins}, nil
}
