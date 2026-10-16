package workqueue

import "errors"

// QueueName 表示队列位置；不代表 Pod 或整个作业的运行状态。
type QueueName string

const (
	QueueNone      QueueName = "None" // 尚未入队，或从队列管理中移除。
	QueueActive    QueueName = "Active"
	QueueFail      QueueName = "Fail"
	QueueRetry     QueueName = "Retry"
	QueueFastRetry QueueName = "FastRetry"
)

// TransitionRequest 描述一次迁移意图，不让调用方分别操作源、目标队列。
// 迁移保持同一个单元身份和成员范围；拆分或产生新一轮单元是另一项操作。
type TransitionRequest struct {
	Unit       Unit
	From       QueueName // 调用方预期的源位置，实现时须核对，不能盲信。
	To         QueueName
	Reason     string
	Message    string
	WaitingFor string // 进入 retry 时携带的等待条件，具体结构后续细化。
}

var ErrTransitionNotImplemented = errors.New("队列迁移尚未实现")

// Transition 是队列间迁移的统一入口，目前仅定义契约，不修改任何队列。
// 后续实现需满足：
//   - 校验单元身份、预期源位置和目标准入条件，包括快速重试的严格准入。
//   - 在 WorkQueue 层协调源移除、目标写入及位置记录；单个队列的锁不足以保证整体一致。
//   - 处理重复请求及过期请求，不能丢失单元或让同一单元同时等待在多个队列。
//   - 将纯队列迁移与 Pod 绑定、业务回滚分开，这里不执行 API 操作或 Agent 请求。
//
// Pop 后的处理中状态、完成退出及事件再次到达如何衔接，后续另行讨论；
// QueueNone 不代表“正在调度”，当前定义还不是完整的调度状态机。
func (w *WorkQueue) Transition(request TransitionRequest) error {
	// TODO：实现统一迁移及一致性校验；占位阶段明确返回错误，不能伪装成功。
	return ErrTransitionNotImplemented
}
