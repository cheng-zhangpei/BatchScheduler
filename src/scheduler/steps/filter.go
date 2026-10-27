package steps

import (
	"context"
	"fmt"

	"batchscheduler/src/group"
	"batchscheduler/src/scheduler"
	"batchscheduler/src/scheduler/upstream"
	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
	corev1 "k8s.io/api/core/v1"
)

// Filter 读取本轮成员，再从共享资源账取一次快照，交给已有 GroupFilter。
// 资源快照包含本地暂占，不需要等这些 Pod 的绑定通知从 informer 返回。
func (r *Runner) Filter(ctx context.Context, a *Attempt) (map[workqueue.PodRef]scheduler.PodFilterResult, error) {
	if r.views == nil || r.resources == nil {
		return nil, fmt.Errorf("过滤缺少组读取器或共享资源账")
	}
	pods, err := r.members(a.Request)
	if err != nil {
		return nil, err
	}
	snapshot := r.resources.Snapshot()
	single, err := upstream.NewPodFilterFromSnapshot(ctx, snapshot.Nodes)
	if err != nil {
		return nil, err
	}
	results, err := NewGroupFilter(single).Filter(ctx, pods)
	if err != nil {
		return nil, err
	}
	a.members, a.filter = pods, single
	a.boundPeers = nil
	for _, pod := range snapshot.BoundPods {
		owner, belongs := group.GroupForPod(pod)
		// 这些位置来自本轮实际集群视图。已删除、已结束的 Pod 只留历史，不再作为运行锚点。
		if belongs && owner == a.Request.Unit.Group && pod.Spec.NodeName != "" && pod.DeletionTimestamp == nil && pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed {
			a.boundPeers = append(a.boundPeers, pod.DeepCopy())
		}
	}
	return results, nil
}

// members 按本轮冻结的 Pod 引用取完整对象，不把同组其他 Unit 的成员也带进来。
func (r *Runner) members(request staging.Execution) ([]*corev1.Pod, error) {
	key := request.Unit.Group
	view, err := r.views.Get(key.Namespace, key.Name)
	if err != nil {
		return nil, fmt.Errorf("读取执行组: %w", err)
	}
	if view == nil || view.Group == nil || view.Group.Namespace != key.Namespace || view.Group.Name != key.Name || view.Group.UID != key.UID || view.Group.DeletionTimestamp != nil {
		return nil, fmt.Errorf("执行组已变化或正在删除")
	}
	current := make(map[workqueue.PodRef]*corev1.Pod, len(view.Members))
	for _, member := range view.Members {
		if member.Group == key && member.Pod != nil {
			current[workqueue.PodRef{Name: member.Pod.Name, UID: member.Pod.UID}] = member.Pod
		}
	}
	pods := make([]*corev1.Pod, 0, len(request.Members))
	for _, ref := range request.Members {
		pod := current[ref]
		// 旧引用不能拿到同名新 Pod，也不能重新调度已经绑定或结束的 Pod。
		if pod == nil || pod.Spec.NodeName != "" || pod.DeletionTimestamp != nil || (pod.Status.Phase != "" && pod.Status.Phase != corev1.PodPending) {
			return nil, fmt.Errorf("成员 %s 已变化、已绑定或已结束", ref.Name)
		}
		pods = append(pods, pod.DeepCopy())
	}
	return pods, nil
}
