// Package staging 记录每个 Unit 做到哪一步，并安排它接下来做什么。
// 真实调度、绑定和重新入队的部分还没全部接好，阅读说明见 docs/buffer.md。
//
// 两条并行路径汇入这里：
//
//	Active / FastRetry 出队 → Accept：提交待执行的冻结 Unit。
//	Informer → 感知层 → Observe：提交集群中 Pod/Group 的变化。
//
// informer 可以先看到 Pod，之后这个 Pod 才被装进 Unit、通过 Accept 进入这里。
// Observe 告诉我们“集群是什么样”，Report 告诉我们“刚才让执行器做的事做完没有”。
package staging

import (
	"context"

	"batchscheduler/src/group"
	"batchscheduler/src/workqueue"
	"k8s.io/apimachinery/pkg/types"
)

// Center 列出缓冲区对外提供的方法；具体实现是 buffer 包中的 Buffer。
type Center interface {
	// Accept 接收已经出队的 Unit：建一份执行记录，记住哪些 Pod 正由它处理。
	// 同一个 Unit 重复送来时不重复建记录。这里不计算节点，也不绑定。
	// TODO：补齐 Active.Pop → Accept；尤其明确出队后 Accept 失败该把任务放回哪里。
	Accept(context.Context, workqueue.Unit) error

	// Observe 根据组编号读取 informer 缓存，更新 Record 中的 Pod 观察信息。
	// 没有 Record 也通知评估：可能有新 Pod 需要入队。不会因此凭空创建 Record。
	Observe(context.Context, Observation) error

	// Report 接收执行器的结果，更新执行进度，再通知评估下一步。
	// 用操作编号核对结果来自哪一次请求；超时但不知道成败时，先保留等待标记。
	// 返回 error 是“这份结果没处理好”；Feedback.Failure 是“执行的那件事失败了”。
	Report(context.Context, Feedback) error

	// Evaluate 检查这个组的任务：继续执行、等待、结束，还是迁移队列？
	// 只返回 Decision 列表，不修改 Record、不实际清理，也不移动队列里的任务。
	Evaluate(context.Context, group.Key) ([]Decision, error)

	// Apply 按 Decision 调用 Select、Transition、执行器或结束处理。
	// 真实执行器完成后再调用 Report；Apply 返回 nil 不代表 Pod 已经绑定。
	Apply(context.Context, Decision) error

	// Release 先确认本轮可以结束，再让队列清掉旧关联，最后删除 Buffer 的记录。
	// 它不删除 Pod/CR，也不撤销资源预占；这些工作必须先由执行器处理完。
	Release(context.Context, workqueue.UnitKey) error

	// ContainsPod 查询“这个 Pod 是否已经被 Buffer 中某个 Unit 接住”。
	// Select 用它避免重复组装。只查本地 map，不在这里反过来调用 WorkQueue。
	ContainsPod(group.Key, types.UID) bool
}
