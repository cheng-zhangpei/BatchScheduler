package staging

import (
	"batchscheduler/src/group"
	errorengine "batchscheduler/src/staging/error_engine"
	"batchscheduler/src/workqueue"
	corev1 "k8s.io/api/core/v1"
)

// evaluateRecord 检查一个 Unit 下一步做什么，只返回一个 Decision，不修改 Buffer。
// 外面的 Evaluate 对各 Unit 分别调用它，所以一批可以同时有人完成、有人失败、有人等待。
func evaluateRecord(key workqueue.UnitKey, record *Record, invalidGroup bool) Decision {
	decision := Decision{Group: key.Group, Unit: &key, Revision: record.Revision, Action: ActionWait}

	// 1. 先检查能不能继续判断：操作没做完就等，观察信息不足也先等。
	if reason := evaluationWaitReason(record); reason != "" {
		decision.Reason = reason
		return decision
	}
	facts := summarizeAttempt(record, invalidGroup)
	if facts.unknownMember {
		decision.Reason = "MemberObservationUnknown"
		return decision
	}

	// 2. 报喜：识别已经成功的本轮。只产生 Release，不在这里删除记录。
	if completed, ok := evaluateCompletion(decision, record, facts); ok {
		return completed
	}

	// 3. 报忧：本轮过期或明确失败，交给错误引擎决定如何处理。
	if problem, ok := evaluateErrors(decision, record, facts); ok {
		return problem
	}

	// 4. 正常推进：决定调度、预占、绑定，或者等待下一次反馈。
	return evaluateProgress(decision, record, facts)
}

// evaluationWaitReason 返回“为什么现在要等”，没有原因则返回空字符串。
func evaluationWaitReason(record *Record) string {
	if record.PendingAction != "" {
		// 防止第一次操作未结束，就因重复通知再执行一次。
		return "OperationInFlight"
	}
	for _, state := range record.Members {
		if state.Phase == group.PodBinding || state.Phase == group.PodBindingUnknown {
			// 防止绑定结果未知时重绑或撤销仍在使用的预留。
			return "BindingResultPending"
		}
	}
	if record.Observed == nil {
		return "AwaitingObservation"
	}
	return ""
}

// attemptFacts 把这一轮的情况收集到一起，方便后面的阶段判断。
// 它只是本次函数调用的临时变量，不再多存一份长期任务记录。
type attemptFacts struct {
	members        []workqueue.PodRef
	failedMembers  []workqueue.PodRef
	failure        *group.FailureInfo
	invalidated    bool
	unknownMember  bool
	allBound       bool
	anyBound       bool
	allUnscheduled bool
}

// summarizeAttempt 逐个看成员，汇总是否都绑定、是否有失败、本轮是否已经过期。
// 这里只收集情况，后面的函数才决定怎么办。
func summarizeAttempt(record *Record, invalidGroup bool) attemptFacts {
	facts := attemptFacts{
		members: record.Unit.Members(), failure: record.Failure,
		invalidated: invalidGroup || !record.Observed.Exists || record.Observed.Deleting,
		allBound:    true, allUnscheduled: true,
	}
	for _, ref := range facts.members {
		observed, internal := record.Observed.Members[ref], record.Members[ref]
		if !observed.Known && !facts.invalidated {
			facts.unknownMember = true
			return facts
		}
		terminalUnbound := observed.NodeName == "" && (observed.Status.Phase == corev1.PodSucceeded || observed.Status.Phase == corev1.PodFailed)
		if observed.Known && (!observed.Present || observed.Deleting || terminalUnbound) {
			// 原成员已不能执行旧计划；已绑定 Pod 的业务结束不在此处当作调度失败。
			facts.invalidated = true
		}
		facts.allBound = facts.allBound && observed.Known && observed.Present && observed.NodeName != ""
		facts.allUnscheduled = facts.allUnscheduled && internal.Phase == group.PodUnscheduled
		memberFailure := internal.LastFailure
		if memberFailure == nil && record.NextOperation == "" && len(record.Plan) == 0 {
			// 没有执行器确认的成员错误时，使用 Observe 映射出的错误。
			// 节点已经算出来后，不让尚未刷新的旧 PodScheduled=False 推翻新计划。
			memberFailure = observed.Failure
		}
		if memberFailure != nil {
			facts.failedMembers = append(facts.failedMembers, ref)
			if facts.failure == nil {
				facts.failure = memberFailure
			}
		}
	}
	// 看看是否已经有成员分到了节点，正常推进时不能再拿旧计划对它重复绑定。
	for _, observed := range record.Observed.Members {
		facts.anyBound = facts.anyBound || observed.NodeName != ""
	}
	return facts
}

// evaluateCompletion 检查能否结束本轮：都已绑定，且没有失败或过期问题。
// 没有异常且都绑定了但收尾未完成时，返回 Wait；全部收尾后才返回 Release。
func evaluateCompletion(d Decision, record *Record, facts attemptFacts) (Decision, bool) {
	if facts.invalidated || facts.failure != nil || !facts.allBound {
		return d, false
	}
	d.Reason = "MembersBoundAwaitingSettlement"
	if record.ResponsibilitiesSettled {
		d.Action, d.Reason = ActionRelease, "AttemptComplete"
	}
	return d, true
}

// evaluateErrors 把已知错误交给错误引擎，再给结果加上 Unit 编号和 Record 版本。
// Observe 已把支持的 Pod 调度异常映射成 FailureInfo；未支持的原因仍需逐条补规则。
func evaluateErrors(d Decision, record *Record, facts attemptFacts) (Decision, bool) {
	members := facts.failedMembers
	if facts.invalidated || record.Unit.Mode() == workqueue.AllTogether || record.Failure != nil {
		// Atomic 处理整轮；整操作错误无法定位成员时，Partial 也处理整轮。
		members = facts.members
	}
	result, ok := errorengine.Decide(errorengine.Input{
		Invalidated: facts.invalidated, Failure: facts.failure, Members: members,
		RecoveryComplete: record.RecoveryComplete, ResponsibilitiesSettled: record.ResponsibilitiesSettled,
	})
	if !ok {
		return d, false
	}
	d.Reason, d.Failure, d.Members = result.Reason, result.Failure, result.Members
	switch result.Action {
	case errorengine.Recover:
		d.Action, d.Operation = ActionExecute, OperationRecover
	case errorengine.ReturnFail:
		d.Action, d.To = ActionReturn, workqueue.QueueFail
	case errorengine.Release:
		d.Action = ActionRelease
	}
	return d, true
}

// evaluateProgress 处理正常任务：第一次安排 Schedule，以后按 NextOperation 继续。
func evaluateProgress(d Decision, record *Record, facts attemptFacts) Decision {
	if record.NextOperation != "" || facts.allUnscheduled {
		if facts.anyBound {
			// 防止外部已经绑定部分成员后，继续按旧计划预占或重复绑定。
			d.Reason = "PartialBindingAwaitingFeedback"
			return d
		}
		if record.NextOperation != "" {
			d.Action, d.Operation, d.Reason = ActionExecute, record.NextOperation, "ContinueExecution"
		} else {
			d.Action, d.Operation, d.Reason = ActionExecute, OperationSchedule, "ReadyToSchedule"
		}
		return d
	}
	d.Reason = "AwaitingExecutionFeedback"
	return d
}
