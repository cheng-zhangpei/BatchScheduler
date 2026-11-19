package workqueue

import (
	"time"

	"batchscheduler/src/group"
	"k8s.io/apimachinery/pkg/types"
)

// UnitPhase 描述处理进度，QueueName 描述等待位置，两者分开记录。
type UnitPhase string

const (
	UnitUnknown        UnitPhase = ""
	UnitNew            UnitPhase = "New"            // 正在组装，可追加成员，尚未入队。
	UnitFrozen         UnitPhase = "Frozen"         // 成员已固定，正在首次入队或出队交接。
	UnitQueued         UnitPhase = "Queued"         // 位于四个队列之一，等待对应处理。
	UnitProcessing     UnitPhase = "Processing"     // 已取出，正在执行本轮处理。
	UnitAwaitingResult UnitPhase = "AwaitingResult" // 操作结果未知，须确认后才能重试。
	UnitDone           UnitPhase = "Done"           // 本轮处理结束，不等同于业务任务成功。
	UnitCancelled      UnitPhase = "Cancelled"      // 本轮已失效或取消。
)

// UnitState 是 Unit 内部的生命周期记录，不是独立组件。
// UnitNew 与 Queued/Active 可以追加成员；固定名单后交给 Buffer 执行。
type UnitState struct {
	FailedAttempts map[types.UID]int // 各成员失败次数，随 Unit 转交，供下一轮退避使用。
	Revision       uint64            // 生命周期版本；队列迁移决策必须针对它，不使用 Center.Record.Revision。
	Phase          UnitPhase
	Queue          QueueName
	LastFailure    *group.FailureInfo
	WaitingFor     string
	// RetryAfter 记录从 Fail 迁入时登记的时间。Retry 条目的 Plan.NotBefore 是实际门槛，
	// 也覆盖部分成员直接回送 Retry 的情况。
	RetryAfter *time.Time
}

// 生命周期约束：
//   - Active 等待中可以追加；出队交接时固定名单，执行中不能追加。
//   - UnitQueued 对应一个明确队列；处理中、结果待确认、结束及取消时 Queue 为 QueueNone。
//   - 同一 Pod UID 同时最多归属一个未结束单元，由 PodState.UnitID 关联。
//   - 旧单元回调不得覆盖 Pod 已经归属的新单元状态。
//   - 首次入队由 Select 协调状态和实际队列；队列间迁移由 Transition 协调。
//   - Select 只管理待调度成员；Atomic 到齐和执行结果由 Buffer 判断。
