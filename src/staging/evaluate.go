package staging

import (
	"context"
	"fmt"
	"sort"

	"batchscheduler/src/group"
	"batchscheduler/src/workqueue"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// Evaluate 以组为决策范围，扫描本组记录；不跨组扫描，不修改记录或执行动作。
// 单个 Partial Unit 的失败不阻止其他 Unit 推进；Atomic 的失败范围是整个 Unit。
// compaction 暂只保留候选范围：不合并、不改冻结成员、不迁移成员归属。
// TODO：已交给 Fail/Retry 的单元须通过 workqueue 提供的组级快照另行评估，
// 不能继续伪装为 Center 持有；队列侧 Transition 的事件/时间接线尚未实现。
func (b *Buffer) Evaluate(ctx context.Context, key group.Key) ([]Decision, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if key.Namespace == "" || key.Name == "" || key.UID == "" || b.views == nil {
		return nil, fmt.Errorf("Evaluate 缺少组身份或缓存读取器")
	}
	view, err := b.views.Get(key.Namespace, key.Name)
	if err != nil && !apierrors.IsNotFound(err) {
		return nil, err
	}
	exists := err == nil && view != nil && view.Group != nil && (group.Key{Namespace: view.Group.Namespace, Name: view.Group.Name, UID: view.Group.UID}) == key
	b.mu.RLock()
	defer b.mu.RUnlock()
	keys := make([]workqueue.UnitKey, 0, len(b.groups[key]))
	for unitKey := range b.groups[key] {
		keys = append(keys, unitKey)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].ID < keys[j].ID })
	decisions := make([]Decision, 0, len(keys)+1)
	for _, unitKey := range keys {
		record := b.records[unitKey]
		invalidGroup := !exists || view.Group.DeletionTimestamp != nil
		if exists {
			mode := workqueue.SubmitMode(view.Group.Spec.SubmitMode)
			if mode == "" {
				mode = workqueue.AllTogether
			}
			invalidGroup = invalidGroup || mode != record.Unit.Mode() || view.Group.Generation != record.Generation
		}
		decisions = append(decisions, evaluateRecord(unitKey, record, invalidGroup))
	}
	// 无本地记录也必须能处理观察：有未绑定、未被 Center 持有的成员时，
	// 建议调用已有 Select 组装。Select 自己继续检查队列归属，不重复造 Unit。
	// Unit=nil 的决策不持有新执行归属；Apply 接线时须重读最新 View。
	if exists && view.Group.DeletionTimestamp == nil {
		for _, member := range view.Members {
			pod := member.Pod
			if pod == nil || member.Group != key || pod.Spec.NodeName != "" || pod.DeletionTimestamp != nil || (pod.Status.Phase != "" && pod.Status.Phase != corev1.PodPending) {
				continue
			}
			if _, held := b.owners[key][pod.UID]; !held {
				decisions = append(decisions, Decision{Group: key, Action: ActionReturn, Reason: "AssessUnheldMembers"})
				break
			}
		}
	}
	return decisions, nil
}

func evaluateRecord(key workqueue.UnitKey, record *Record, invalidGroup bool) Decision {
	decision := Decision{Group: key.Group, Unit: &key, Revision: record.Revision, Action: ActionWait}
	// 在途/未知结果优先于失败和删除；不能一边请求还在运行，一边再次执行或释放。
	if record.PendingAction != "" {
		decision.Reason = "OperationInFlight"
		return decision
	}
	for _, state := range record.Members {
		if state.Phase == group.PodBinding || state.Phase == group.PodBindingUnknown {
			decision.Reason = "BindingResultPending"
			return decision
		}
	}
	if record.Observed == nil {
		decision.Reason = "AwaitingObservation"
		return decision
	}
	refs := record.Unit.Members()
	failed := make([]workqueue.PodRef, 0)
	allBound, allUnscheduled := true, true
	obsolete := invalidGroup || !record.Observed.Exists || record.Observed.Deleting
	failure := record.Failure
	for _, ref := range refs {
		fact, state := record.Observed.Members[ref], record.Members[ref]
		if !fact.Known {
			// 组删除/重建时已知旧计划失效，但不能据此确认旧 Pod 或绑定结果。
			if !obsolete {
				decision.Reason = "MemberObservationUnknown"
				return decision
			}
		}
		terminalUnbound := fact.NodeName == "" && (fact.Status.Phase == corev1.PodSucceeded || fact.Status.Phase == corev1.PodFailed)
		if fact.Known && (!fact.Present || fact.Deleting || terminalUnbound) {
			// 成员生命周期结束不等于临时调度失败，先恢复/结清旧责任，不重排终态 Pod。
			obsolete = true
		}
		// 已绑定 Pod 很快运行结束也不能反推调度失败，更不能因此回滚其他成员。
		// 本轮先完成绑定/资源交接；业务运行失败是否创建新 Pod 由后续生命周期策略处理。
		allBound = allBound && fact.Known && fact.Present && fact.NodeName != ""
		allUnscheduled = allUnscheduled && state.Phase == group.PodUnscheduled
		if state.LastFailure != nil {
			failed = append(failed, ref)
			if failure == nil {
				failure = state.LastFailure
			}
		}
	}
	if obsolete {
		decision.Reason = "AttemptInvalidated"
		decision.Members = append([]workqueue.PodRef(nil), refs...)
		if record.ResponsibilitiesSettled {
			decision.Action = ActionRelease
		} else {
			decision.Action, decision.Operation = ActionExecute, OperationRecover
		}
		return decision
	}
	if failure != nil {
		copyFailure := *failure
		decision.Failure = &copyFailure
		decision.Reason = failure.Reason
		// Atomic 扩为整轮；Unit 级错误无法定位失败成员时，Partial 也先处理整轮。
		if record.Unit.Mode() == workqueue.AllTogether || record.Failure != nil {
			failed = append([]workqueue.PodRef(nil), refs...)
		}
		decision.Members = failed
		if !record.RecoveryComplete || !record.ResponsibilitiesSettled {
			decision.Action, decision.Operation = ActionExecute, OperationRecover
		} else {
			decision.Action, decision.To = ActionReturn, workqueue.QueueFail
			// TODO：FastRetry 严格准入独立讨论，不按失败数量直接升级优先级。
			// 回送及新成员范围组织交给 Apply/Select；这里不能修改冻结 Unit。
		}
		return decision
	}
	if allBound {
		decision.Reason = "MembersBoundAwaitingSettlement"
		if record.ResponsibilitiesSettled {
			decision.Action, decision.Reason = ActionRelease, "AttemptComplete"
		}
		return decision
	}
	if record.NextOperation != "" {
		for _, fact := range record.Observed.Members {
			if fact.NodeName != "" {
				// 计划生成后出现部分外部绑定，不能继续按旧计划预留或重复绑定。
				decision.Reason = "PartialBindingAwaitingFeedback"
				return decision
			}
		}
		decision.Action, decision.Operation, decision.Reason = ActionExecute, record.NextOperation, "ContinueExecution"
		return decision
	}
	if allUnscheduled {
		// 排队后部分成员已绑定时不能拿整轮重新调度，等待执行记录对齐。
		for _, fact := range record.Observed.Members {
			if fact.NodeName != "" {
				decision.Reason = "PartialBindingAwaitingFeedback"
				return decision
			}
		}
		decision.Action, decision.Operation, decision.Reason = ActionExecute, OperationSchedule, "ReadyToSchedule"
		return decision
	}
	// 调度计划、Assume/绑定交接的反馈协议尚未实现，不能仅凭 Reserved 自动绑定。
	decision.Reason = "AwaitingExecutionFeedback"
	return decision
}
