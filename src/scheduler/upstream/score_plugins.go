package upstream

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nodeaffinity"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/noderesources"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/tainttoleration"
)

type weightedScorePlugin struct {
	plugin framework.ScorePlugin
	weight int64
}

// newUpstreamScorePlugins 先固定四个基础插件，各权重为 1；这不是原生默认配置的完整复刻。
// 资源评分暂用 LeastAllocated，CPU/内存等权；后续需要改策略时从这里看起。
// 这些 PreScore/Score 路径直接读取传入的 NodeInfo，不需要启动原生调度器或提供 Handle。
func newUpstreamScorePlugins(ctx context.Context) ([]weightedScorePlugin, error) {
	features := feature.NewSchedulerFeaturesFromGates(utilfeature.DefaultFeatureGate)
	features.EnableDRAExtendedResource = false
	resources := []config.ResourceSpec{{Name: "cpu", Weight: 1}, {Name: "memory", Weight: 1}}
	factories := []struct {
		new  func(context.Context, runtime.Object, framework.Handle, feature.Features) (framework.Plugin, error)
		args runtime.Object
	}{
		{noderesources.NewFit, &config.NodeResourcesFitArgs{ScoringStrategy: &config.ScoringStrategy{Type: config.LeastAllocated, Resources: resources}}},
		{noderesources.NewBalancedAllocation, &config.NodeResourcesBalancedAllocationArgs{Resources: resources}},
		{nodeaffinity.New, &config.NodeAffinityArgs{}},
		{tainttoleration.New, nil},
	}
	plugins := make([]weightedScorePlugin, 0, len(factories))
	for _, factory := range factories {
		plugin, err := factory.new(ctx, factory.args, nil, features)
		if err != nil {
			return nil, fmt.Errorf("初始化原生评分插件: %w", err)
		}
		plugins = append(plugins, weightedScorePlugin{plugin: plugin.(framework.ScorePlugin), weight: 1})
	}
	return plugins, nil
}
