// Package informer 观察 Kubernetes 对象，将变化交给资源缓存和组观察入口。
package informer

import (
	"context"
	"fmt"

	scheduling "batchscheduler/src/apis/scheduling/v1alpha1"
	"batchscheduler/src/group"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

// Informers 共享一个 factory，后续模块通过 lister 读取其缓存。
// lister 返回的是共享对象，调用方不得直接修改。
type Informers struct {
	factory        informers.SharedInformerFactory
	groupFactory   dynamicinformer.DynamicSharedInformerFactory
	Nodes          corelisters.NodeLister
	Pods           corelisters.PodLister
	PodGroups      cache.GenericLister
	GroupViews     *group.Store
	podInformer    cache.SharedIndexInformer
	groupInformer  cache.SharedIndexInformer
	synced         []cache.InformerSynced
	resourceErrors chan error // 资源记账失败时停止运行，避免继续使用不完整的资源账。
}

func New(client kubernetes.Interface, groupClient dynamic.Interface) (*Informers, error) {
	// 0 关闭定期 resync；底层仍通过 List/Watch 维护缓存和处理断线重连。
	factory := informers.NewSharedInformerFactory(client, 0)
	nodes := factory.Core().V1().Nodes()
	pods := factory.Core().V1().Pods()

	// 显式创建两个 informer，确保它们在 factory.Start 时会被启动。
	nodeInformer := nodes.Informer()
	podInformer := pods.Informer()
	// 自定义 CR 通过 dynamic informer 观察，无需先生成专用 clientset。
	groupFactory := dynamicinformer.NewDynamicSharedInformerFactory(groupClient, 0)
	podGroups := groupFactory.ForResource(scheduling.PodGroupGVR) // 我们拿到一种特定资源的方式就是 GVR
	groupInformer := podGroups.Informer()
	views, err := group.NewStore(groupInformer.GetIndexer(), podInformer.GetIndexer())
	if err != nil {
		return nil, err
	}
	return &Informers{
		factory:        factory, // 这是用来统一的管理所有的informer的，如果每次我要一个个确认很麻烦
		Nodes:          nodes.Lister(),
		Pods:           pods.Lister(),
		groupFactory:   groupFactory,
		PodGroups:      podGroups.Lister(),
		GroupViews:     views,
		podInformer:    podInformer,
		groupInformer:  groupInformer,
		resourceErrors: make(chan error, 1),
		// hasSync是首次同步的标记，不是staleness的标记
		synced: []cache.InformerSynced{nodeInformer.HasSynced, podInformer.HasSynced, groupInformer.HasSynced},
	}, nil
}

// Run 等 informer 缓存和已注册的资源记账初始回调都完成，再通知入口启动调度。
func (i *Informers) Run(ctx context.Context, ready chan<- struct{}) error {
	ctx, stop := context.WithCancel(ctx)
	i.factory.Start(ctx.Done())
	i.groupFactory.Start(ctx.Done())
	defer func() {
		stop()
		i.groupFactory.Shutdown()
		i.factory.Shutdown()
	}()
	if !cache.WaitForCacheSync(ctx.Done(), i.synced...) {
		return fmt.Errorf("Pod/Node/PodGroup informer 初始缓存未完成同步: %w", ctx.Err())
	}
	select {
	case err := <-i.resourceErrors:
		return fmt.Errorf("初始化调度资源账: %w", err)
	default:
	}
	close(ready)
	select {
	case <-ctx.Done():
		return nil
	case err := <-i.resourceErrors:
		return fmt.Errorf("更新调度资源账: %w", err)
	}
}
