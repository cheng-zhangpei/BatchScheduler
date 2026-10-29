// Package steps 实现执行线中的具体步骤，不负责决定步骤的先后顺序。
package steps

import (
	"time"

	"batchscheduler/src/scheduler/planner"
	"batchscheduler/src/scheduler/plugins/communication"
	"batchscheduler/src/scheduler/resourcecache"
	"batchscheduler/src/scheduler/upstream"
	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
)

// Runner 持有步骤需要的数据读取器。Executor 直接调用它的方法。
type Runner struct {
	Groups      staging.GroupContextReader // 启动前接入 Buffer，查询同组其他轮次。
	Planner     planner.Planner            // 默认接通信搜索并按其排序选定计划；其他语义的最终取舍待实现。
	Client      kubernetes.Interface       // 真实 Bind 使用的客户端，在启动前设置。
	BindTimeout time.Duration              // 一轮绑定最多等待多久。
	views       workqueue.GroupViewReader
	resources   *resourcecache.Cache // 与 informer 写入、后续 Assume 使用的是同一份资源账。
}

func NewRunner(views workqueue.GroupViewReader, resources *resourcecache.Cache) *Runner {
	return &Runner{views: views, resources: resources, Planner: planner.Planner{Placement: communication.New()}, BindTimeout: 30 * time.Second}
}

// Attempt 是正在执行的这一轮，执行结束后不长期保存。
// 步骤一旦产生资源占用或发出绑定请求，就必须更新标记，供失败后的 Recover 判断。
type Attempt struct {
	Request      staging.Execution
	Plan         map[workqueue.PodRef]string
	Reserved     bool               // 仍有本地预留需要结清。
	Binding      bool               // 已发出绑定，但还没有确认全部结果。
	BoundMembers []workqueue.PodRef // 本轮已经得到 API 成功响应的成员，失败恢复时不能当成未绑定。

	// Filter 保存本轮成员和节点视图，Score 直接复用，不再读取另一份缓存。
	members    []*corev1.Pod
	filter     *upstream.PodFilter
	boundPeers []*corev1.Pod
}
