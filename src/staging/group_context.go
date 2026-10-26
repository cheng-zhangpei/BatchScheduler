package staging

import (
	"context"
	"time"

	"batchscheduler/src/group"
	"batchscheduler/src/workqueue"
)

// SuccessfulRecord 保存一轮调度成功时的记录，不保存 Unit 接口、锁或运行中的请求。
// ID 是这份 Record 的唯一编号；Unit.ID 在程序重启后可能重复，不能单独当数据库主键。
type SuccessfulRecord struct {
	ID         string
	Unit       workqueue.UnitKey
	Mode       workqueue.SubmitMode
	Generation int64
	Revision   uint64
	SavedAt    time.Time
	Members    []SuccessfulMember
}

type SuccessfulMember struct {
	Pod      workqueue.PodRef
	State    group.PodState
	Observed ObservedPod // 成功时看见的状态；不是 Pod 现在仍在运行的保证。
}

// SuccessHistory 将成功记录落盘，并按完整组身份查询。实现位于 staging/history。
type SuccessHistory interface {
	Save(context.Context, SuccessfulRecord) error
	List(context.Context, group.Key) ([]SuccessfulRecord, error)
}

// InFlightUnit 说明同组另一轮正在做到哪里。Plan 只是计划，不能当成已绑定的位置。
type InFlightUnit struct {
	RecordID string
	Unit     workqueue.UnitKey
	Mode     workqueue.SubmitMode
	Phase    ExecutionPhase
	Members  []workqueue.PodRef
	Plan     map[workqueue.PodRef]string
}

// GroupContext 让多个 Unit 共享组级信息；不合并冻结的 Unit，也不改变其成员归属。
type GroupContext struct {
	Group     group.Key
	InFlight  []InFlightUnit
	Succeeded []SuccessfulRecord
}

type GroupContextReader interface {
	GroupContext(context.Context, group.Key) (GroupContext, error)
}
