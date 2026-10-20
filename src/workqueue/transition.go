package workqueue

import (
	"errors"
	"fmt"
	"time"

	"batchscheduler/src/group"
)

// QueueName 表示队列位置；不代表 Pod 或整个作业的运行状态。
type QueueName string

const (
	QueueNone      QueueName = "None" // 尚未入队，或从队列管理中移除。
	QueueActive    QueueName = "Active"
	QueueFail      QueueName = "Fail"
	QueueRetry     QueueName = "Retry"
	QueueFastRetry QueueName = "FastRetry"
)

func isQueue(queue QueueName) bool {
	switch queue {
	case QueueActive, QueueFail, QueueRetry, QueueFastRetry:
		return true
	default:
		return false
	}
}

// TransitionRequest 描述一次迁移意图，不让调用方分别操作源、目标队列。
// 迁移保持同一个单元身份和成员范围；拆分或产生新一轮单元是另一项操作。
type TransitionRequest struct {
	Unit       Unit
	From       QueueName // 调用方预期的源位置，实现时须核对，不能盲信。
	To         QueueName
	Reason     string
	Message    string
	WaitingFor string // 保留说明字段；Fail → Retry 的结构化条件由 Fail 原因生成，不盲信此值。
	// RecoveryComplete 由恢复/缓冲区路径明确确认：没有在途或结果未知的绑定，
	// 需要撤销的预留已清理，且本轮固定成员仍可重试；无须清理也要明确置 true。
	// 当前是调用方契约，不代表 Transition 已实现实际资源/集群校验。
	RecoveryComplete bool
	RetryAfter       time.Time  // 时间型重试必填；事件型可选，时间和事件条件须同时满足。
	Wake             *RetryWake // 仅 Retry → Active 使用；由事件入口或定时入口提供。
}

var ErrTransitionNotImplemented = errors.New("队列迁移尚未实现")

// Transition 实现 Fail → Retry 和 Retry → Active，FastRetry 不参与。
// 失败由缓冲区确认后经 Select 放回 Fail，不实现 Active → Fail。
// 与 Select 使用同一把管理锁；调用方不能绕过管理入口并发修改队列/Unit 状态。
// 原始队列 Get 不是跨队列一致快照，后续消费者也须通过统一管理协议接入。
// 这里不调用 API、不回滚资源、不拆分单元；成员需变化时交回缓冲区组织新单元。
func (w *WorkQueue) Transition(request TransitionRequest) error {
	if request.From == QueueRetry && request.To == QueueActive {
		return w.activateRetry(request)
	}
	if request.From != QueueFail || request.To != QueueRetry {
		return ErrTransitionNotImplemented
	}
	if request.Unit == nil {
		return fmt.Errorf("迁移缺少 Unit")
	}
	if !request.RecoveryComplete {
		return fmt.Errorf("上一轮结果或恢复未确认完成，不能进入 Retry")
	}
	w.selectMu.Lock()
	defer w.selectMu.Unlock()
	key := request.Unit.Key()
	failure, exists := w.Fail.Get(key)
	if !exists {
		// 完全相同的请求重放安全，不重置退避时间，也不覆盖已存在的等待计划。
		if retry, ok := w.Retry.Get(key); ok && retry.Unit.State().Queue == QueueRetry {
			state := retry.Unit.State()
			if (request.Reason == "" || (state.LastFailure != nil && request.Reason == state.LastFailure.Reason)) && retry.Plan.NotBefore.Equal(request.RetryAfter) {
				return nil
			}
		}
		return fmt.Errorf("单元 %s 不在预期的 Fail 队列或请求已过期", key.ID)
	}
	// 以 Fail 中保存的对象和错误为准，不信任请求传入的对象状态或错误覆盖。
	unit := failure.Unit
	if request.Reason != "" && request.Reason != failure.Reason {
		return fmt.Errorf("迁移请求的失败原因与当前 Fail 记录不匹配")
	}
	state := unit.State()
	if state.Queue != QueueFail || state.Phase != UnitQueued {
		return fmt.Errorf("Fail 条目与单元状态不一致")
	}
	if _, exists := w.Retry.Get(key); exists {
		return fmt.Errorf("单元已存在于 Retry，拒绝覆盖")
	}
	if w.isStaged(unit) {
		return fmt.Errorf("单元仍由缓冲区持有，不能进入 Retry")
	}
	record := w.groups[key.Group]
	if record == nil || record.units[key.ID] == nil || record.group.Phase == group.GroupDeleting {
		return fmt.Errorf("单元缺少有效的组归属或组正在删除")
	}
	for _, member := range unit.Members() {
		pod := record.pods[member.UID]
		if pod == nil || pod.UnitID != key.ID || pod.Phase != group.PodUnscheduled {
			return fmt.Errorf("成员归属或状态不允许重试，需先完成结果确认和清理")
		}
	}
	plan, err := planRetry(failure.Reason, request.RetryAfter)
	if err != nil {
		return err
	}
	if err := unit.MarkRetry(plan); err != nil {
		return err
	}
	// 当前 Put/Remove 无失败返回；管理入口被串行化，状态登记成功后完成条目搬移。
	w.Retry.Put(Retry{Unit: unit, WaitingFor: string(plan.Condition), Plan: plan})
	w.Fail.Remove(key)
	return nil
}
