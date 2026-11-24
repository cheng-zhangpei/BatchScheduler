package resourcecache

import (
	"time"

	"batchscheduler/src/workqueue"
)

// BindingCheckKey 指明要核查的是哪个 Pod 的哪次执行。
// 同名新 Pod 或后来一次执行，都有自己的记录。
type BindingCheckKey struct {
	Unit     workqueue.UnitKey
	ActionID string
	Pod      workqueue.PodRef
}

type BindingCheckState string

const (
	// BindingCheckPending：绑定已超时，仍保留暂占，等待主动查询结果。
	BindingCheckPending BindingCheckState = "Pending"
	// BindingCheckReleased：已按核查策略释放暂占，保留记录以识别晚到结果。
	// 这不表示服务端确认绑定失败。
	BindingCheckReleased BindingCheckState = "Released"
)

// BindingCheck 只记录核查进度，不保存完整 Pod，也不代替 assumed 中的资源明细。
// 后续查询必须核对 Key 中的 Pod UID；查询失败不能增加 UnboundChecks。
type BindingCheck struct {
	Key           BindingCheckKey
	NodeName      string // 原计划节点；核查和补发都沿用它。
	State         BindingCheckState
	TimedOutAt    time.Time // 重试因次数或总期限耗尽，转入后台核查的时间。
	LastCheckedAt time.Time // 最近一次成功查询原 Pod 状态的时间。
	UnboundChecks int       // 连续确认原 UID 仍未绑定的次数。
	ReleasedAt    time.Time // 实际撤销暂占后才填写。
}

// BindingCheckPolicy 定义核查间隔、回收阈值和历史容量。
// 达到次数后才考虑释放；每次有效核查之间至少间隔 CheckInterval。
type BindingCheckPolicy struct {
	RequiredUnboundChecks int
	CheckInterval         time.Duration
	ReleasedCapacity      int // 仅限制已释放记录的 LRU，不淘汰仍有暂占的记录。
}

// DefaultBindingCheckPolicy 给后续接线提供初始值，不改变现有 30 秒绑定超时。
func DefaultBindingCheckPolicy() BindingCheckPolicy {
	return BindingCheckPolicy{
		RequiredUnboundChecks: 5,
		CheckInterval:         time.Second,
		ReleasedCapacity:      1024,
	}
}
