package informer

import (
	"batchscheduler/src/workqueue"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
)

// OnRetryChange 在 Run 前接好重试通知。Node 变化和已绑定 Pod 删除由资源记账回调通知。
// 只表示条件可能改善，不保证能够调度；不在回调执行 Transition。
// TODO：DependencyNotReady 对应的依赖资源 informer 尚未实现。
func (i *Informers) OnRetryChange(notify func(workqueue.RetryCondition)) error {
	_, err := i.podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(old, current interface{}) {
			before, okBefore := old.(*corev1.Pod)
			after, okAfter := current.(*corev1.Pod)
			if okBefore && okAfter && len(before.Spec.SchedulingGates) > 0 && len(after.Spec.SchedulingGates) == 0 {
				// SchedulingGated 等待的门已经被移除，通知评估，不在这里直接重试。
				notify(workqueue.RetryOnDependency)
			}
		},
	})
	if err != nil {
		return err
	}
	i.notifyRetry = notify
	return nil
}
