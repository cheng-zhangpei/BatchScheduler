package workqueue

import (
	"time"

	"batchscheduler/src/group"
)

// UnitPhase 描述处理进度，QueueName 描述等待位置，两者分开记录。
type UnitPhase string

const (
	UnitUnknown        UnitPhase = ""
	UnitNew            UnitPhase = "New"            // 正在组装，可追加成员，尚未入队。
	UnitFrozen         UnitPhase = "Frozen"         // 成员已固定，尚未入队。
	UnitQueued         UnitPhase = "Queued"         // 位于四个队列之一，等待对应处理。
	UnitProcessing     UnitPhase = "Processing"     // 已取出，正在执行本轮处理。
	UnitAwaitingResult UnitPhase = "AwaitingResult" // 操作结果未知，须确认后才能重试。
	UnitDone           UnitPhase = "Done"           // 本轮处理结束，不等同于业务任务成功。
	UnitCancelled      UnitPhase = "Cancelled"      // 本轮已失效或取消。
)

// UnitState 是 Unit 内部的生命周期记录，不是独立组件。
// 仅 UnitNew 阶段允许追加成员；冻结后继续流转，不回到组装阶段。
// 当前接入创建、冻结和首次入队，后续阶段留待统一流转入口维护。
type UnitState struct {
	Phase       UnitPhase
	Queue       QueueName
	LastFailure *group.FailureInfo
	WaitingFor  string
	// RetryAfter 是调度器决定的最早重试时间，不从 Pod.phase 推断。
	// nil 表示没有时间门槛；即使时间已到，也可能仍需等待资源等条件。
	RetryAfter *time.Time
}

// 待实现的约束：
//   - 入队前必须冻结；排队、处理、等待结果及结束状态都不能追加成员。
//   - UnitQueued 对应一个明确队列；处理中、结果待确认、结束及取消时 Queue 为 QueueNone。
//   - 同一 Pod UID 同时最多归属一个未结束单元，由 PodState.UnitID 关联。
//   - 旧单元回调不得覆盖 Pod 已经归属的新单元状态。
//   - 首次入队由 Select 协调状态和实际队列；队列间迁移由 Transition 协调。
//   - Select 结合组内部状态、Pod 事实及单元记录决策；具体策略仍待实现。
