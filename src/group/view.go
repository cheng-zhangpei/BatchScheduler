// Package group 将组声明与成员 Pod 组织成调度器可读取的视图。
package group

import (
	"fmt"
	"sort"

	scheduling "batchscheduler/src/apis/scheduling/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
)

const podGroupIndex = "podGroupUID"

// Key 用 UID 区分同名组的不同生命周期，关联只在同一 namespace 内成立。
type Key struct {
	Namespace string
	Name      string
	UID       types.UID
}

// PodView 保留完整 Pod 事实及所属组，不在这里判断是否允许入队。
type PodView struct {
	Pod   *corev1.Pod
	Group Key
}

// View 这个结构就是我们调度器在进行决策时候所看到的视图之一了
type View struct {
	Group   *scheduling.PodGroup
	Members []PodView
}

// GroupForPod 以 controller ownerReference 作为唯一关联来源。
// 未来 operator 创建 Pod 时应设置此引用；这里不根据名字或相同标签猜测归属。
func GroupForPod(pod *corev1.Pod) (Key, bool) {
	owner := metav1.GetControllerOf(pod)
	if owner == nil || owner.APIVersion != scheduling.GroupVersion.String() || owner.Kind != "PodGroup" || owner.Name == "" || owner.UID == "" {
		return Key{}, false
	}
	return Key{Namespace: pod.Namespace, Name: owner.Name, UID: owner.UID}, true
}

// Store 复用 informer 的缓存与索引，不维护第二套成员列表。
// Store的作用是调用cache里面的pod的indexer和group的indexer将两者联系起来。
type Store struct {
	groups cache.Indexer
	pods   cache.Indexer
}

// NewStore 必须在 informer 启动前调用，以便先安装成员索引。
func NewStore(groups, pods cache.Indexer) (*Store, error) {
	// 传入动态和静态的indexer

	if err := pods.AddIndexers(cache.Indexers{
		podGroupIndex: func(obj interface{}) ([]string, error) {
			pod, ok := obj.(*corev1.Pod)
			if !ok {
				return nil, fmt.Errorf("成员索引收到非 Pod 对象: %T", obj)
			}
			key, belongs := GroupForPod(pod)
			if !belongs {
				return nil, nil
			}
			return []string{indexKey(key)}, nil
		},
	}); err != nil {
		return nil, fmt.Errorf("注册 PodGroup 成员索引: %w", err)
	}
	return &Store{groups: groups, pods: pods}, nil
}

// Get 在查询时组装视图。两个缓存之间不是原子快照，未来提交调度决策前需重查。
// 返回的对象均与缓存脱离，避免调用方修改共享对象。
func (s *Store) Get(namespace, name string) (*View, error) {
	obj, exists, err := s.groups.GetByKey(namespace + "/" + name)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, apierrors.NewNotFound(scheduling.PodGroupGVR.GroupResource(), name)
	}
	raw, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil, fmt.Errorf("组缓存收到非 Unstructured 对象: %T", obj)
	}
	declaration := new(scheduling.PodGroup)
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(raw.DeepCopy().Object, declaration); err != nil {
		return nil, fmt.Errorf("解析 PodGroup %s/%s: %w", namespace, name, err)
	}
	if declaration.UID == "" || declaration.Spec.Members <= 0 {
		return nil, fmt.Errorf("PodGroup %s/%s 缺少 UID 或 members 非正数", namespace, name)
	}
	key := Key{Namespace: declaration.Namespace, Name: declaration.Name, UID: declaration.UID}
	// 拿到对应podGroup的pod
	objects, err := s.pods.ByIndex(podGroupIndex, indexKey(key))
	if err != nil {
		return nil, err
	}
	// 组装view
	view := &View{Group: declaration, Members: make([]PodView, 0, len(objects))}
	for _, obj := range objects {
		pod := obj.(*corev1.Pod) // 索引函数已检查类型。
		memberKey, belongs := GroupForPod(pod)
		if belongs && memberKey == key {
			view.Members = append(view.Members, PodView{Pod: pod.DeepCopy(), Group: memberKey})
		}
	}
	// 索引返回顺序不固定；按名字排序使阅读与后续决策输入可复现。
	sort.Slice(view.Members, func(i, j int) bool { return view.Members[i].Pod.Name < view.Members[j].Pod.Name })
	return view, nil
}

func indexKey(key Key) string {
	return key.Namespace + "/" + string(key.UID)
}
