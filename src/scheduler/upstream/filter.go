package upstream

import (
	"context"
	"fmt"

	"batchscheduler/src/scheduler"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nodeaffinity"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nodename"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nodeports"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/noderesources"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nodeunschedulable"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/tainttoleration"
)

// PodFilter 调用 third_party 中未经修改的六个原生插件。
// 我们只准备输入、依次调用 PreFilter/Filter、转换结果，不另写一套过滤规则。
type PodFilter struct {
	nodes   []fwk.NodeInfo
	plugins []framework.FilterPlugin
}

var _ scheduler.PodFilter = (*PodFilter)(nil)

// NewPodFilter 为一轮组过滤准备固定的节点视图。
// pods 必须包含集群中所有已分配的 Pod，而不只是我们负责的组成员。
// 该入口保留给独立适配测试；运行路径使用 NewPodFilterFromSnapshot 读取共享资源账。
// 新一轮或组内试放改变资源后，需要用更新后的视图重新构建；本对象不会自行刷新。
func NewPodFilter(ctx context.Context, nodes []*corev1.Node, pods []*corev1.Pod) (*PodFilter, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	infos, err := buildNodeInfos(nodes, pods)
	if err != nil {
		return nil, err
	}
	plugins, err := newUpstreamFilterPlugins(ctx)
	if err != nil {
		return nil, err
	}
	return &PodFilter{nodes: infos, plugins: plugins}, nil
}

// Filter 分两步：PreFilter 为这个 Pod 准备数据，再逐节点调用 Filter。
// 一个 Pod 一份 CycleState，不能把 A 的资源需求或亲和性准备结果用到 B 身上。
// 当前遍历全部输入节点，不使用上游“大集群找到足够候选就停止”的搜索优化。
func (f *PodFilter) Filter(ctx context.Context, pod *corev1.Pod) (scheduler.PodFilterResult, error) {
	if err := ctx.Err(); err != nil {
		return scheduler.PodFilterResult{}, err
	}
	if pod == nil {
		return scheduler.PodFilterResult{}, fmt.Errorf("原生过滤需要 Pod")
	}
	if err := checkSupportedConstraints(pod); err != nil {
		return scheduler.PodFilterResult{}, err
	}
	state := framework.NewCycleState()
	result := scheduler.PodFilterResult{Rejected: make(map[string]scheduler.NodeRejection)}
	skipped, err := f.preFilter(ctx, state, pod, result.Rejected)
	if err != nil {
		return scheduler.PodFilterResult{}, err
	}
	for _, node := range f.nodes {
		if err := ctx.Err(); err != nil {
			return scheduler.PodFilterResult{}, err
		}
		name := node.Node().Name
		if _, rejected := result.Rejected[name]; rejected {
			continue
		}
		for _, plugin := range f.plugins {
			if skipped[plugin.Name()] {
				continue
			}
			status := plugin.Filter(ctx, state, pod, node)
			if status.IsSuccess() {
				continue
			}
			if !status.IsRejected() {
				return scheduler.PodFilterResult{}, fmt.Errorf("原生 Filter %s: %w", plugin.Name(), status.AsError())
			}
			// 和上游一样，一个插件拒绝这个节点就停止，不再跑此节点的其他插件。
			result.Rejected[name] = upstreamRejection(plugin.Name(), status)
			break
		}
		if _, rejected := result.Rejected[name]; !rejected {
			result.Candidates = append(result.Candidates, name)
		}
	}
	return result, nil
}

// preFilter 还要处理两种原生结果：Skip 表示本 Pod 不需要跑对应 Filter；
// NodeNames 表示后续只能检查指定节点。忽略它们会丢状态或错误放宽候选范围。
func (f *PodFilter) preFilter(ctx context.Context, state *framework.CycleState, pod *corev1.Pod, rejected map[string]scheduler.NodeRejection) (map[string]bool, error) {
	skipped := make(map[string]bool)
	for _, plugin := range f.plugins {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		pre, ok := plugin.(framework.PreFilterPlugin)
		if !ok {
			continue
		}
		res, status := pre.PreFilter(ctx, state, pod, f.nodes)
		if status.IsSkip() {
			skipped[plugin.Name()] = true
			continue
		}
		if !status.IsSuccess() {
			if !status.IsRejected() {
				return nil, fmt.Errorf("原生 PreFilter %s: %w", plugin.Name(), status.AsError())
			}
			for _, node := range f.nodes {
				rejected[node.Node().Name] = upstreamRejection(plugin.Name(), status)
			}
			return skipped, nil
		}
		if !res.AllNodes() {
			for _, node := range f.nodes {
				if !res.NodeNames.Has(node.Node().Name) {
					if _, exists := rejected[node.Node().Name]; !exists {
						rejected[node.Node().Name] = scheduler.NodeRejection{Plugin: plugin.Name(), Reasons: []string{"node excluded by PreFilter"}, Unresolvable: true}
					}
				}
			}
		}
	}
	return skipped, nil
}

func upstreamRejection(plugin string, status *fwk.Status) scheduler.NodeRejection {
	return scheduler.NodeRejection{Plugin: plugin, Reasons: append([]string(nil), status.Reasons()...), Unresolvable: status.Code() == fwk.UnschedulableAndUnresolvable}
}

// newUpstreamFilterPlugins 只初始化本次支持的插件，不启动原生调度器。
// 这六个插件的 PreFilter/Filter 不需要 framework.Handle；Score、事件回调等路径不在此调用。
func newUpstreamFilterPlugins(ctx context.Context) ([]framework.FilterPlugin, error) {
	features := feature.NewSchedulerFeaturesFromGates(utilfeature.DefaultFeatureGate)
	features.EnableDRAExtendedResource = false // DRA 需要额外资源管理器，当前没有接入。
	factories := []struct {
		new  func(context.Context, runtime.Object, framework.Handle, feature.Features) (framework.Plugin, error)
		args runtime.Object
	}{
		{nodename.New, nil},
		{nodeunschedulable.New, nil},
		{tainttoleration.New, nil},
		{nodeaffinity.New, &config.NodeAffinityArgs{}},
		{nodeports.New, nil},
		{noderesources.NewFit, &config.NodeResourcesFitArgs{ScoringStrategy: &config.ScoringStrategy{
			Type: config.LeastAllocated, Resources: []config.ResourceSpec{{Name: "cpu", Weight: 1}, {Name: "memory", Weight: 1}},
		}}},
	}
	plugins := make([]framework.FilterPlugin, 0, len(factories))
	for _, factory := range factories {
		plugin, err := factory.new(ctx, factory.args, nil, features)
		if err != nil {
			return nil, fmt.Errorf("初始化原生过滤插件: %w", err)
		}
		plugins = append(plugins, plugin.(framework.FilterPlugin))
	}
	return plugins, nil
}
