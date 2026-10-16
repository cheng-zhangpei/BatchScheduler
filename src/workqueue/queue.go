// Package workqueue 管理弹性调度单元的处理机会、失败与重试。
package workqueue

import (
	"context"

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

// Retry 保存组正在等待的条件，当前先用文字表达，之后再细化条件类型。
// 此处不引入退避时长，也不假设任意集群事件都能唤醒该组。
type Retry struct {
	Unit       Unit
	WaitingFor string
}

// RetryQueue 保存等待条件变化的组；条件判断及回到 active 的动作尚未实现。
type RetryQueue interface {
	Put(Retry)
	Get(UnitKey) (Retry, bool)
	Remove(UnitKey)
}

// WorkQueue 组织三个基础队列和一个快速重试通道。
// 跨队列流转、单元互斥及消费顺序尚未实现。
// 调用方应通过 Transition 请求队列间迁移，不自行拼接 Remove 和 Add/Put。
// 当前字段仍公开供学习查看；实现迁移时再收紧写入边界。
type WorkQueue struct {
	Active ActiveQueue
	Fail   FailQueue
	Retry  RetryQueue
	// FastRetry 放已获准优先再尝试的单元，复用 FIFO 存取机制。
	// TODO：定义严格准入条件，以及最高优先级如何避免挤压普通单元。
	FastRetry ActiveQueue
}

func New() *WorkQueue {
	return &WorkQueue{
		Active:    newActiveQueue(),
		Fail:      &failQueue{items: make(map[UnitKey]Failure)},
		Retry:     &retryQueue{items: make(map[UnitKey]Retry)},
		FastRetry: newActiveQueue(),
	}
}

// Select 是组视图到队列的边界，尚未实现，也尚未接入事件回调。
func (w *WorkQueue) Select(view *group.View) {
	// TODO：队列内部再次校验，不依赖入口过滤已经通过；具体校验规则待讨论。
	// TODO：从组视图确定本轮 Unit，再选择队列；不在这里提前固定拆分与流转规则。
	// TODO：确定目标后，通过 Transition 统一提交入队意图。
}
