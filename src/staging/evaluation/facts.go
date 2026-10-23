package evaluation

import (
	"batchscheduler/src/group"
	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
	corev1 "k8s.io/api/core/v1"
)

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
func summarizeAttempt(record *staging.Record, invalidGroup bool) attemptFacts {
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
		if memberFailure == nil && record.ExecutionPhase == "" && len(record.Plan) == 0 {
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
