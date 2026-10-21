package informer

import (
	"reflect"

	"batchscheduler/src/workqueue"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
)

// OnRetryChange 把节点容量/条件/放置属性变化与已绑定 Pod 释放提示送给评估入口。
// 只表示条件可能改善，不保证能够调度；不在回调执行 Transition。
// TODO：DependencyNotReady 对应的依赖资源 informer 尚未实现。
func (i *Informers) OnRetryChange(notify func(workqueue.RetryCondition)) error {
	nodeChanged := func(interface{}) {
		notify(workqueue.RetryOnResources)
		notify(workqueue.RetryOnNodes)
		notify(workqueue.RetryOnPlacement)
	}
	_, err := i.factory.Core().V1().Nodes().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: nodeChanged, DeleteFunc: nodeChanged,
		UpdateFunc: func(old, current interface{}) {
			before, okBefore := old.(*corev1.Node)
			after, okAfter := current.(*corev1.Node)
			if okBefore && okAfter && (!reflect.DeepEqual(before.Spec, after.Spec) ||
				!reflect.DeepEqual(before.Status, after.Status) || !reflect.DeepEqual(before.Labels, after.Labels)) {
				nodeChanged(current)
			}
		},
	})
	if err != nil {
		return err
	}
	_, err = i.podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(old, current interface{}) {
			before, okBefore := old.(*corev1.Pod)
			after, okAfter := current.(*corev1.Pod)
			if okBefore && okAfter && len(before.Spec.SchedulingGates) > 0 && len(after.Spec.SchedulingGates) == 0 {
				// SchedulingGated 等待的门已经被移除，通知评估，不在这里直接重试。
				notify(workqueue.RetryOnDependency)
			}
		},
		DeleteFunc: func(obj interface{}) {
			if pod, ok := deletedObject(obj).(*corev1.Pod); ok && pod.Spec.NodeName != "" {
				notify(workqueue.RetryOnResources)
			}
		},
	})
	return err
}
