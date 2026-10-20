// Package staging 定义调度中心的接收、观察、评估、动作分发、反馈和释放。
// 模拟执行器闭环已验证；实际调度/绑定及队列交接适配器仍待实现。
//
// 两条并行路径汇入这里：
//
//	Active / FastRetry 出队 → Accept：提交待执行的冻结 Unit。
//	Informer → 感知层 → Observe：提交集群中 Pod/Group 的变化。
//
// Observe 不依赖此前发生过 Accept：调度器重启前已绑定的 Pod、已经结束本轮
// 跟踪的 Group，以及 operator 新创建的成员，都可能先从观察路径到达。
// 执行器产生的本轮结果通过 Report 返回，与集群观察分开表达。
package staging

import (
	"context"

	"batchscheduler/src/group"
	"batchscheduler/src/workqueue"
	"k8s.io/apimachinery/pkg/types"
)

// Center 是调度中心的行为边界；Buffer 已实现接口，外部操作由 Handlers 注入。
type Center interface {
	// Accept 接收从 Active/FastRetry 取出的冻结 Unit，登记本轮记录和成员归属。
	// 接收成功后才可以派发调度；相同交接重放不能重复启动执行。
	// TODO：与 workqueue 补齐出队、处理中状态及交接失败的恢复协议。
	Accept(context.Context, workqueue.Unit) error

	// Observe 接收感知层提交的当前资源观察，独立于 Accept 路径。
	// 不要求有本地 Unit；无执行记录时也须评估是否需要组织新任务或忽略变化，
	// 不能仅因为“查不到本轮记录”而丢弃观察，也不能把历史 Pod 自动标成在途。
	// 观察可关联并更新已有记录；迟到、重复事件不得覆盖较新的执行事实。
	// Store 仍是 informer 缓存的读取视图，此方法不向 Store 再写一份资源。
	Observe(context.Context, Observation) error

	// Report 接收调度/绑定/恢复执行器的反馈，必须对应明确的本轮身份。
	// 方法返回的 error 表示反馈处理失败；反馈内的 Failure 表示被执行操作失败。
	// 请求超时等未知结果不能直接当作已失败并释放归属，须等待进一步确认。
	Report(context.Context, Feedback) error

	// Evaluate 根据组视图和内部记录提出决策，不执行 API、绑定或队列写入。
	// 以组为入口，既支持没有本地 Unit 的观察，也支持同组存在多个 Partial Unit。
	// 按 Atomic/Partial 确定失败范围；不凭容器状态推断调度失败，不执行 compaction。
	Evaluate(context.Context, group.Key) ([]Decision, error)

	// Apply 执行决策或派发给相应组件；执行结果通过 Report/Observe 回来。
	// 调度算法、Assume/Forget、绑定客户端仍由各自组件实现。
	// 回送队列走 Select；已在 Fail/Retry 的队列间迁移才走 Transition。
	// 执行前须检查决策依据是否已变化，重复调用不得重复绑定或重复组织 Unit。
	// 返回 nil 不代表异步操作已完成；须登记在途动作并等待结果。
	Apply(context.Context, Decision) error

	// Release 仅释放本轮内存记录、成员归属和组关联索引，不代替资源回滚。
	// 绑定结果未知、仍有执行器在途、预留未结清或回送未交接完成时必须拒绝。
	// 成功绑定须完成预留向实际占用的交接，失败须完成必要清理，才能释放。
	// 不删除 CR/Pod；Group 仍存在也不要求永久保存已结束的 Unit。
	// 与 workqueue 的旧归属解除须协调，防止一边删除后另一边仍占用成员。
	Release(context.Context, workqueue.UnitKey) error

	// ContainsPod 提供 workqueue.StagingLookup 边界：是否持有该成员的执行归属。
	// 必须快速、并发安全且不得回调 workqueue；仅观察过某 Pod 不等于持有它。
	// 因此 Observe 可在无本地 Unit 时到达，而不会自动阻止后续 Select 组装。
	ContainsPod(group.Key, types.UID) bool
}
