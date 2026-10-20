package informer

import (
	"batchscheduler/src/group"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/client-go/tools/cache"
)

// OnGroupChange 必须在 Run 前注册。回调通知 Observe 更新本地事实，不运行调度或 API。
// Pod/CR 两个 informer 的回调可以并发到达；notify 必须并发安全。
// 回调中的缓存已更新，但两种资源缓存并非原子同步；Observe 须读取最新 Store。
// notify 必须保持轻量；绑定/恢复等慢操作不能直接进入此回调。
// CR 和 Pod 都发通知，确保初始 List 顺序不同、Pod 改组和同名 CR 重建时能重查。
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

// Watch 删除可能只有墓碑；仍取旧对象身份去通知，不能忽略尚未结清的旧归属。
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
