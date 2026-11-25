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
	// BindingCheckPending：绑定尚未确认，在原预算内保留暂占，等待成功确认或到期。
	BindingCheckPending BindingCheckState = "Pending"
	// BindingCheckReleased：预算到期已释放暂占，保留记录以识别晚到结果。
	// 这不表示服务端确认绑定失败。
	BindingCheckReleased BindingCheckState = "Released"
	// BindingCheckGone：原 UID 已删除或被替换，不再等待旧绑定。
	BindingCheckGone BindingCheckState = "Gone"
)

// BindingCheck 保存原计划和唯一的绑定截止时间，不保存完整 Pod 或观测次数。
type BindingCheck struct {
	Key        BindingCheckKey
	NodeName   string // 原计划节点；核查和补发都沿用它。
	State      BindingCheckState
	Deadline   time.Time // 从首次 Bind 开始计算；转后台、回报重试都沿用它。
	ReleasedAt time.Time // 实际撤销暂占后才填写。
}

// BindingCheckPolicy 只定义截止时间检查频率和历史容量，不增加第二段回收门槛。
type BindingCheckPolicy struct {
	CheckInterval    time.Duration
	ReleasedCapacity int // 仅限制已释放记录的 LRU，不淘汰仍有暂占的记录。
}

// DefaultBindingCheckPolicy 提供默认截止时间检查频率和历史容量。
func DefaultBindingCheckPolicy() BindingCheckPolicy {
	return BindingCheckPolicy{
		CheckInterval:    time.Second,
		ReleasedCapacity: 1024,
	}
}
