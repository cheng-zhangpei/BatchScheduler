// Package errorengine 把错误按类型分给不同函数，返回下一步安排，不直接执行。
package errorengine

import (
	"time"

	"batchscheduler/src/group"
	"batchscheduler/src/workqueue"
)

// Input 告诉错误引擎：发生了什么、哪些成员受影响、之前的恢复是否做完。
// Queued 非 nil 表示任务还在 Fail/Retry 中；否则处理 Buffer 中正在执行的 Unit。
type Input struct {
	Queued                  *workqueue.QueuedUnit
	Now                     time.Time
	Wake                    *workqueue.RetryWake
	Invalidated             bool
	Failure                 *group.FailureInfo
	Members                 []workqueue.PodRef
	RecoveryComplete        bool
	ResponsibilitiesSettled bool
}

type Action string

const (
	Transition Action = "Transition"
	Recover    Action = "Recover"
	ReturnFail Action = "ReturnFail"
	Release    Action = "Release"
)

// Decision 是错误引擎的处理意见；staging 再加上 Unit 编号和版本，交给 Apply。
// 这里只返回错误处理意见；evaluation 包再加上 Unit 编号和版本，组成完整决定。
type Decision struct {
	Transition *workqueue.TransitionRequest
	Action     Action
	Reason     string
	Failure    *group.FailureInfo
	Members    []workqueue.PodRef
}

// Decide 是错误处理的唯一入口。第二个返回值为 true 表示产生了决定。
// false 表示没有产生决定：可能没有错误，也可能暂时不允许重试。
// Pod 调度状态由 MapPodSchedulingError 转成内部 Reason，执行器也使用内部 Reason。
func Decide(input Input) (Decision, bool) {
	if input.Queued != nil {
		return decideQueuedError(input)
	}
	if input.Invalidated {
		return handleInvalidatedAttempt(input), true
	}
	if input.Failure == nil {
		return Decision{}, false
	}
	switch input.Failure.Reason {
	case workqueue.ReasonUnschedulable:
		return handleRetryableFailure(input), true
	case workqueue.ReasonInsufficientResources, workqueue.ReasonNodeUnavailable,
		workqueue.ReasonPlacementConstraints, workqueue.ReasonDependencyNotReady,
		workqueue.ReasonTransientSchedulingError, workqueue.ReasonBindingRejected:
		return handleRetryableFailure(input), true
	case workqueue.ReasonInvalidSpec, workqueue.ReasonForbidden:
		return handleConfigurationFailure(input), true
	default:
		return handleOtherFailure(input), true
	}
}

// handleInvalidatedAttempt 处理原组或成员已不能继续使用的情况：先恢复，再结束旧 Unit。
func handleInvalidatedAttempt(input Input) Decision {
	d := Decision{Action: Recover, Reason: "AttemptInvalidated", Members: append([]workqueue.PodRef(nil), input.Members...)}
	if input.ResponsibilitiesSettled {
		d.Action = Release
	}
	return d
}

// handleRetryableFailure 处理资源不足等可能再次尝试的错误：本轮先恢复，再回 Fail。
// 不是现在直接重试；任务回到队列后，decideFailedQueue 再安排 Fail → Retry。
func handleRetryableFailure(input Input) Decision {
	return recoverThenFail(input)
}

// handleConfigurationFailure 处理无效声明、权限问题：先恢复，再回 Fail 等外部修复。
// 它和上面的 handler 目前共用同一段收尾；区别体现在队列阶段不允许它自动重试。
func handleConfigurationFailure(input Input) Decision {
	return recoverThenFail(input)
}

// handleOtherFailure 当前对其余 Reason 也先恢复再回 Fail，没有自动重试资格。
// TODO：BindingUnknown 如果被错误地当作已确认 Failure 写进来，还缺专门的确认处理。
// 正常的未知结果应由 Feedback.Unknown 保留 Pending，不能在这里假装确认失败。
func handleOtherFailure(input Input) Decision {
	return recoverThenFail(input)
}

// recoverThenFail：没恢复完就安排 Recover；恢复和资源收尾都完成后才回 Fail。
func recoverThenFail(input Input) Decision {
	failure := *input.Failure
	d := Decision{Action: Recover, Reason: failure.Reason, Failure: &failure, Members: append([]workqueue.PodRef(nil), input.Members...)}
	if input.RecoveryComplete && input.ResponsibilitiesSettled {
		d.Action = ReturnFail
	}
	return d
}
