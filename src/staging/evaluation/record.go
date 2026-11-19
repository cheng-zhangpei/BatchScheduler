package evaluation

import (
	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
)

// EvaluateRecord 检查一个 Unit 下一步做什么，只返回一个 Decision，不修改 Buffer。
// 外面的 Evaluate 对各 Unit 分别调用它，所以一批可以同时有人完成、有人失败、有人等待。
func EvaluateRecord(key workqueue.UnitKey, record *staging.Record, invalidGroup bool) staging.Decision {
	decision := staging.Decision{Group: key.Group, Unit: &key, Revision: record.Revision, Action: staging.ActionWait}
	// 这个分支是这样的，比如一个partial组，AB已经binding了，CD资源不够暂时还没有binding，这个时候record中标注returning送回queue里面
	// 这个时候遇到这个标识，我们就直接wait不要再次回送就好了，不然workqueue又要拦截一次浪费性能
	if record.ReturningMembers {
		decision.Reason = "ReturningMembers"
		return decision
	}
	// 未放置成员没有参与绑定，可以单独交回；其他成员即使仍在执行，也不用一起等待。
	// 这些unplaced的成员没有assume也没有暂占资源，啥时候往回交呢，需要一部分的节点已经binding出去了，这部分节点就返回过来交给队列重新调度
	if partial, ok := evaluateUnplacedMembers(decision, record, invalidGroup); ok {
		return partial
	}

	// Bind 已返回未知结果时，Informer 看到的绑定可以补上那次丢失的确认。因为bind结果不知道。
	if _, confirmed := ObservedBindingSuccess(record); confirmed && !invalidGroup {
		decision.Action, decision.Reason = staging.ActionRelease, "AttemptComplete"
		return decision
	}

	// 这轮现在要等什么？上一件事还没结账。执行器报告了阻塞，就返回 ExecutionBlocked；否则返回 OperationInFlight
	//有成员处于 PodBinding 或 PodBindingUnknown
	// 还没拿到集群观测，比如observe为nil，说明这个record集群都还没反馈，那我们就不要去执行
	if reason := evaluationWaitReason(record); reason != "" {
		decision.Reason = reason
		return decision
	}
	// 这个函数本质就是对比集群实际运行姿态和调度器的record之间的差距，并把这个差距记录下来给后面进行决策的时候使用
	facts := summarizeAttempt(record, invalidGroup)

	if facts.unknownMember {
		decision.Reason = "MemberObservationUnknown"
		return decision
	}
	// Atomic 的正常启动和失败处理都以完整成员为一轮；后到成员仍要能汇入。
	// 组已删除、成员已失效时则不能继续等齐，仍交给下面的结束/恢复分支。
	if !facts.invalidated && waitingForAtomicMembers(record, facts) {
		decision.Reason = "AwaitingGroupMembers"
		return decision
	}

	// 2. 报喜：识别已经成功的本轮。只产生 Release，不在这里删除记录，这个地方是由后面bind的逻辑统治的
	if completed, ok := evaluateCompletion(decision, record, facts); ok {
		return completed
	}

	// 3. 报忧：本轮过期或明确失败，交给错误引擎决定如何处理。
	if problem, ok := evaluateErrors(decision, record, facts); ok {
		return problem
	}

	// 4. 只启动整轮执行；预占、绑定等步骤由 Executor 内部推进。
	return evaluateProgress(decision, record, facts)
}
