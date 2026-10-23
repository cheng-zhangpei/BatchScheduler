package steps

import (
	"context"
	"fmt"

	"batchscheduler/src/scheduler"
	"batchscheduler/src/workqueue"
	corev1 "k8s.io/api/core/v1"
)

// GroupFilter 只把单 Pod 过滤封装成批量过滤，不做组内放置决策。
type GroupFilter struct {
	podFilter scheduler.PodFilter
}

func NewGroupFilter(podFilter scheduler.PodFilter) *GroupFilter {
	return &GroupFilter{podFilter: podFilter}
}

// Filter 接收本轮 Unit 的完整 Pod 对象，逐个过滤，按 Pod 引用返回各自结果。
// 调用方负责从组视图中取出本轮成员；这里不把同组其他 Unit 的成员也加入本轮。
//
// 一个 Pod 无候选节点，仍继续收集其他成员的结果，后续组策略再决定怎么处理。
// 过滤程序报错则立即返回 error，不能把“计算失败”当成“节点都不满足要求”。
// 多个成员可以同时把同一个节点列为候选；它们能否一起放下，由后续组内规划判断。
func (f *GroupFilter) Filter(ctx context.Context, pods []*corev1.Pod) (map[workqueue.PodRef]scheduler.PodFilterResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if f.podFilter == nil {
		return nil, fmt.Errorf("GroupFilter 尚未接入单 Pod 过滤器")
	}
	if err := validateFilterPods(pods); err != nil {
		return nil, err
	}
	results := make(map[workqueue.PodRef]scheduler.PodFilterResult, len(pods))
	for _, pod := range pods {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		result, err := f.podFilter.Filter(ctx, pod)
		if err != nil {
			return nil, fmt.Errorf("过滤 Pod %s/%s: %w", pod.Namespace, pod.Name, err)
		}
		results[workqueue.PodRef{Name: pod.Name, UID: pod.UID}] = result
	}
	return results, nil
}

// validateFilterPods 先检查整批成员，避免漏成员、重复过滤或把同名旧 Pod 的结果覆盖掉。
// 是否属于同一个 PodGroup，由调用方通过组视图确认；这里不再维护一份成员关系。
func validateFilterPods(pods []*corev1.Pod) error {
	if len(pods) == 0 {
		return fmt.Errorf("GroupFilter 缺少本轮 Pod")
	}
	names, uids := make(map[string]bool), make(map[string]bool)
	for _, pod := range pods {
		if pod == nil || pod.Namespace == "" || pod.Name == "" || pod.UID == "" {
			return fmt.Errorf("GroupFilter 需要名称、命名空间和 UID 完整的 Pod")
		}
		if pod.Namespace != pods[0].Namespace || names[pod.Name] || uids[string(pod.UID)] {
			return fmt.Errorf("GroupFilter 的成员必须同命名空间，且名称和 UID 不能重复")
		}
		names[pod.Name], uids[string(pod.UID)] = true, true
	}
	return nil
}
