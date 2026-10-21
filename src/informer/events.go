package informer

import (
	"batchscheduler/src/group"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/client-go/tools/cache"
)

// OnGroupChange 在 Run 前注册“组/Pod 变化后要调用的函数”，main 传入的是调用 Observe 的函数。
// informer 先更新自己的缓存，再调用它；Pod 和组的通知可能同时到达，Observe 要能处理。
// 两种缓存不是一起更新的，所以后续事件仍可能补上此前缺少的组或成员信息。
// 这里不执行绑定或恢复，避免一个慢请求挡住后面的资源变化通知。
func (i *Informers) OnGroupChange(notify func(group.Key)) error {
	podChanged := func(obj interface{}) {
		if pod, ok := deletedObject(obj).(*corev1.Pod); ok {
			if key, belongs := group.GroupForPod(pod); belongs {
				notify(key)
			}
		}
	}
	groupChanged := func(obj interface{}) {
		metadata, err := meta.Accessor(deletedObject(obj))
		if err == nil && metadata.GetUID() != "" {
			notify(group.Key{Namespace: metadata.GetNamespace(), Name: metadata.GetName(), UID: metadata.GetUID()})
		}
	}
	if _, err := i.podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: podChanged, DeleteFunc: podChanged,
		UpdateFunc: func(old, current interface{}) { podChanged(old); podChanged(current) },
	}); err != nil {
		return err
	}
	_, err := i.groupInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: groupChanged, DeleteFunc: groupChanged,
		UpdateFunc: func(old, current interface{}) { groupChanged(old); groupChanged(current) },
	})
	return err
}

// deletedObject 处理删除通知的包装对象，从里面取出旧 Pod/组的编号。
// 删除事件即使晚到，也要通知 Observe 重新查看，不能直接忽略。
func deletedObject(obj interface{}) interface{} {
	switch tombstone := obj.(type) {
	case cache.DeletedFinalStateUnknown:
		return tombstone.Obj
	case *cache.DeletedFinalStateUnknown:
		return tombstone.Obj
	default:
		return obj
	}
}
