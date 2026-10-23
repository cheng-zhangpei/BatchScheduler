// Package steps 实现执行线中的具体步骤，不负责决定步骤的先后顺序。
package steps

import (
	"batchscheduler/src/scheduler/upstream"
	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
	corev1 "k8s.io/api/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
)

// Runner 持有步骤需要的数据读取器。Executor 直接调用它的方法。
type Runner struct {
	views workqueue.GroupViewReader
	nodes corelisters.NodeLister
	pods  corelisters.PodLister
}

func NewRunner(views workqueue.GroupViewReader, nodes corelisters.NodeLister, pods corelisters.PodLister) *Runner {
	return &Runner{views: views, nodes: nodes, pods: pods}
}

// Attempt 是正在执行的这一轮，执行结束后不长期保存。
// 步骤一旦产生资源占用或发出绑定请求，就必须更新标记，供失败后的 Recover 判断。
type Attempt struct {
	Request  staging.Execution
	Plan     map[workqueue.PodRef]string
	Reserved bool // 仍有本地预留需要结清。
	Binding  bool // 已发出绑定，但还没有确认全部结果。

	// Filter 保存本轮成员和节点视图，Score 直接复用，不再读取另一份缓存。
	members []*corev1.Pod
	filter  *upstream.PodFilter
}
