// Package workqueue 管理弹性调度单元的处理机会、失败与重试。
package workqueue

import (
	"context"
	"sync"

	"batchscheduler/src/group"
)

// ActiveQueue 保存可以尝试调度的单元，以 UnitKey 对待处理条目去重。
// 调度时再读取 Pod 事实；单元引用不替代对象缓存。
type ActiveQueue interface {
	Add(Unit)
	// Pop 在没有条目时等待；ctx 取消时结束等待并返回错误。
	Pop(ctx context.Context) (Unit, error)
}

// Failure 保存一次失败的原因，不在这里决定是否重试。
type Failure struct {
	Unit    Unit
	Reason  string
	Message string
}

// FailQueue 按单元保留失败记录，Put 更新同单元已有记录。
// 失败分类或子队列留到处理具体错误时讨论。
type FailQueue interface {
	Put(Failure)
	Get(UnitKey) (Failure, bool)
	Remove(UnitKey)
}

// Retry 保存单元的重试门槛，不假设任意集群事件都能唤醒它。
type Retry struct {
	Unit       Unit
	WaitingFor string
	Plan       RetryPlan
	// ChangeObserved 记住本次等待期间的相关变化，避免退避结束前收到的事件丢失。
	ChangeObserved bool
}

// RetryQueue 保存等待条件变化的单元；不自行监听事件或启动定时器。
type RetryQueue interface {
	Put(Retry)
	Get(UnitKey) (Retry, bool)
	Remove(UnitKey)
}

// WorkQueue 组织三个基础队列和一个快速重试通道。
// 跨队列流转、单元互斥及消费顺序尚未实现。
// 调用方应通过 Transition 请求队列间迁移，不自行拼接 Remove 和 Add/Put。
// 首次入队由 Select 直接分发到目标队列。
// 当前字段仍公开供学习查看；实现迁移时再收紧写入边界。
type WorkQueue struct {
	// selectMu 暂时串行保护组装和归属登记，后续再考虑按组拆分锁。
	selectMu sync.Mutex
	groups   map[group.Key]*selectionState
	nextID   uint64
	// Policy 决定目标队列；nil 时仅组装，不猜测尚未确定的队列规则。
	// 须在开始处理事件前配置，不能在 Select 并发执行时替换。
	Policy SelectionPolicy
	// Views 为 Pod 来源补齐组上下文，开始处理事件前配置。
	Views GroupViewReader
	// Staging 由未来暂存区提供，查询须快速且不得回调 WorkQueue。
	// 处理事件前配置；nil 表示暂存区尚未接入。
	Staging StagingLookup
	// MakeUnit 是单元实现的替换入口，默认使用 NewUnit。
	MakeUnit func(UnitKey, SubmitMode) Unit
	Active   ActiveQueue
	Fail     FailQueue
	Retry    RetryQueue
	// FastRetry 放已获准优先再尝试的单元，复用 FIFO 存取机制。
	// TODO：定义严格准入条件，以及最高优先级如何避免挤压普通单元。
	FastRetry ActiveQueue
}

func New() *WorkQueue {
	return &WorkQueue{
		groups:    make(map[group.Key]*selectionState),
		MakeUnit:  NewUnit,
		Policy:    DefaultPolicy,
		Active:    newActiveQueue(),
		Fail:      &failQueue{items: make(map[UnitKey]Failure)},
		Retry:     &retryQueue{items: make(map[UnitKey]Retry)},
		FastRetry: newActiveQueue(),
	}
}
