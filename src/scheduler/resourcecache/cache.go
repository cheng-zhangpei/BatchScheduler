// Package resourcecache 保存调度器共享的节点资源账，包括真实绑定和本地暂占。
// 每轮 Filter 从这里取快照；Assume 在选定计划后更新这里，供下一轮调度看到占用。
// informer 校正真实占用，Filter 取本轮快照，执行器通过 Assume 登记、通过 Forget 撤销本轮暂占。
package resourcecache

import (
	"sync"

	"batchscheduler/src/workqueue"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// Assumption 记录一个 Pod 在哪次执行中暂占了哪个节点。
// Pod 保存独立副本，Spec.NodeName 是选定节点，资源申请和端口声明是记账依据。
// Forget 根据这份记录撤销暂占；确认真实绑定后，实际占用继续保留，只取消暂占身份。
type Assumption struct {
	Unit     workqueue.UnitKey // 哪个调度单元。
	ActionID string            // 该单元的哪次执行。
	Pod      *corev1.Pod       // 带目标节点的完整 Pod 副本，不修改 informer 中的对象。
}

// Cache 跨调度轮次保存节点总账、已记账 Pod 索引和暂占明细。
// 它只管理资源占用；Buffer 仍通过 Report 维护执行进度。
type Cache struct {
	mu sync.RWMutex

	// 节点名 → 资源总账。每个已绑定或暂占的 Pod 都在对应 NodeInfo 中计入一次。
	nodes map[string]*framework.NodeInfo
	// Pod UID → 已记入总账的 Pod。更新或删除时先找旧记录，才能撤销原来的占用。
	pods map[types.UID]*corev1.Pod
	// Pod UID → 暂占明细。Forget 从这里找到所属执行、目标节点和需要撤销的 Pod。
	assumed map[types.UID]Assumption
}

// New 创建空资源账，由 informer 的初始事件填入节点和已有绑定。
func New() *Cache {
	return &Cache{
		nodes:   make(map[string]*framework.NodeInfo),
		pods:    make(map[types.UID]*corev1.Pod),
		assumed: make(map[types.UID]Assumption),
	}
}
