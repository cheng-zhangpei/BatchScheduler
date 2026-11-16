package informer

import (
	"reflect"

	"batchscheduler/src/workqueue"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
)

// ResourceObserver 接收节点和 Pod 变化，具体资源计算交给资源缓存。
type ResourceObserver interface {
	UpdateNode(*corev1.Node)
	RemoveNode(*corev1.Node)
	UpdatePod(*corev1.Pod) error
	RemovePod(*corev1.Pod) error
}

// OnResourceChange 在 Run 前注册资源记账。初始 List 和后续 Watch 使用同一套处理函数。
// ready 不仅等待 informer 的索引填好，也等待这两个处理函数接完初始数据。
func (i *Informers) OnResourceChange(resources ResourceObserver) error {
	notifyNodeChange := func() {
		if i.notifyRetry != nil {
			i.notifyRetry(workqueue.RetryOnResources)
			i.notifyRetry(workqueue.RetryOnNodes)
			i.notifyRetry(workqueue.RetryOnPlacement)
		}
	}
	nodes, err := i.factory.Core().V1().Nodes().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if node, ok := obj.(*corev1.Node); ok {
				resources.UpdateNode(node)
				notifyNodeChange()
			}
		},
		UpdateFunc: func(old, current interface{}) {
			before, okBefore := old.(*corev1.Node)
			after, okAfter := current.(*corev1.Node)
			if okBefore && okAfter {
				// 同一个回调先写资源账，再让等待中的 Unit 重新选点。
				resources.UpdateNode(after)
				if !reflect.DeepEqual(before.Spec, after.Spec) || !reflect.DeepEqual(before.Status, after.Status) ||
					!reflect.DeepEqual(before.Labels, after.Labels) {
					notifyNodeChange()
				}
			}
		},
		DeleteFunc: func(obj interface{}) {
			if node, ok := deletedObject(obj).(*corev1.Node); ok {
				resources.RemoveNode(node)
				notifyNodeChange()
			}
		},
	})
	if err != nil {
		return err
	}
	podChanged := func(obj interface{}) {
		if pod, ok := obj.(*corev1.Pod); ok {
			i.resourceError(resources.UpdatePod(pod))
		}
	}
	pods, err := i.podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    podChanged,
		UpdateFunc: func(_, current interface{}) { podChanged(current) },
		DeleteFunc: func(obj interface{}) {
			if pod, ok := deletedObject(obj).(*corev1.Pod); ok {
				if err := resources.RemovePod(pod); err != nil {
					i.resourceError(err)
					return
				}
				// 先释放资源再唤醒，Retry 重新 Filter 时能看到这次扣账。
				if pod.Spec.NodeName != "" && i.notifyRetry != nil {
					i.notifyRetry(workqueue.RetryOnResources)
				}
			}
		},
	})
	if err != nil {
		return err
	}
	// 注册返回的 HasSynced 包含回调处理进度，单独等待 informer.HasSynced 不够。
	i.synced = append(i.synced, nodes.HasSynced, pods.HasSynced)
	return nil
}

// resourceError 将第一条记账错误交给 Run，不让 informer 的通知线程等待主线程。
func (i *Informers) resourceError(err error) {
	if err == nil {
		return
	}
	select {
	case i.resourceErrors <- err:
	default:
	}
}
