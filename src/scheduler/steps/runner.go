// Package steps 实现执行线中的具体步骤，不负责决定步骤的先后顺序。
package steps

import (
	"batchscheduler/src/scheduler/planner"
	"batchscheduler/src/scheduler/plugins/communication"
	"batchscheduler/src/scheduler/resourcecache"
	"batchscheduler/src/scheduler/upstream"
	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
	corev1 "k8s.io/api/core/v1"
)

// Runner 持有步骤需要的数据读取器。Executor 直接调用它的方法。
type Runner struct {
	Groups    staging.GroupContextReader // 启动前接入 Buffer，查询同组其他轮次。
	Planner   planner.Planner            // 默认接通信搜索并按其排序选定计划；其他语义的最终取舍待实现。
	views     workqueue.GroupViewReader
	resources *resourcecache.Cache // 与 informer 写入、后续 Assume 使用的是同一份资源账。
}

func NewRunner(views workqueue.GroupViewReader, resources *resourcecache.Cache) *Runner {
	return &Runner{views: views, resources: resources, Planner: planner.Planner{Placement: communication.New()}}
}

// Attempt 是正在执行的这一轮，执行结束后不长期保存。
// 步骤一旦产生资源占用或发出绑定请求，就必须更新标记，供失败后的 Recover 判断。
type Attempt struct {
	Request  staging.Execution
	Plan     map[workqueue.PodRef]string
	Reserved bool // 仍有本地预留需要结清。
	Binding  bool // 已发出绑定，但还没有确认全部结果。

	// Filter 保存本轮成员和节点视图，Score 直接复用，不再读取另一份缓存。
	members    []*corev1.Pod
	filter     *upstream.PodFilter
	boundPeers []*corev1.Pod
}
