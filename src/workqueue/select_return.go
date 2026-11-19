package workqueue

import (
	"fmt"
	"time"

	"batchscheduler/src/group"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

// ReturnRequest 是 Buffer 交回的失败成员。Source 直接携带原 Unit 和重试次数，
// 不要求 WorkQueue 保存正在执行的旧 Unit。Buffer 负责确认这些成员已经可以交出。
type ReturnRequest struct {
	Source           Unit
	Members          []PodRef
	Failure          *group.FailureInfo
	RecoveryComplete bool
	KeepPrevious     bool      // 部分回送：Buffer 继续处理 Source 的其他成员。
	To               QueueName // 默认 Fail；可等待集群变化的未放置成员可以直接进入 Retry。
}

// selectReturn 分三步：确认回送成员 → 创建下一轮 → 登记待调度关联并入队。
// 这里只接收 Buffer 给出的结果，不重新决定恢复范围，也不结束 Buffer 的旧 Record。
func (w *WorkQueue) selectReturn(request ReturnRequest) error {
	if request.Source == nil || !request.RecoveryComplete || request.Failure == nil || request.Failure.Reason == "" || len(request.Members) == 0 {
		return fmt.Errorf("失败回送需要原 Unit、成员、错误及恢复完成确认")
	}
	decision, err := returnQueueDecision(request)
	if err != nil {
		return err
	}
	if w.Views == nil {
		return fmt.Errorf("失败回送缺少组视图读取器")
	}
	source := request.Source
	key := source.Key().Group
	view, err := w.Views.Get(key.Namespace, key.Name)
	if err != nil {
		return fmt.Errorf("失败回送读取组视图: %w", err)
	}
	if err := validateReturn(request, view); err != nil {
		return err
	}

	w.selectMu.Lock()
	defer w.selectMu.Unlock()
	// 同一批成员不能同时出现在两个待调度 Unit 中。
	record := w.groups[key]
	if record != nil {
		for _, ref := range request.Members {
			if p := record.pods[ref.UID]; p != nil && p.UnitID != "" {
				return fmt.Errorf("回送成员 %s 已在队列中", ref.Name)
			}
		}
	}
	next, err := w.newUnit(key, source.Mode(), group.DefinitionFor(view.Group))
	if err != nil {
		return err
	}
	attempts := make(map[types.UID]int, len(request.Members))
	previous := source.State().FailedAttempts
	mostFailures := 0
	for _, ref := range request.Members {
		if err := next.AddMember(ref); err != nil {
			return err
		}
		attempts[ref.UID] = previous[ref.UID] + 1
		mostFailures = max(mostFailures, attempts[ref.UID])
	}
	if err := next.SetFailedAttempts(attempts); err != nil {
		return err
	}
	if decision.To == QueueRetry {
		decision.RetryPlan.NotBefore = retryDeadline(time.Now(), mostFailures)
	}
	if err := prepareEnqueue(next, decision); err != nil {
		return err
	}

	record = w.groupRecord(key)
	for _, ref := range request.Members {
		failure := *request.Failure
		record.pods[ref.UID] = &group.PodState{UnitID: next.Key().ID, Phase: group.PodUnscheduled,
			LastFailure: &failure, FailedAttempts: attempts[ref.UID]}
	}
	record.units[next.Key().ID] = next
	record.group.UnitIDs[next.Key().ID] = struct{}{}
	w.publishUnit(next, decision)
	return nil
}

// returnQueueDecision 只解释明确指定的目标队列；它不做执行结果判断。
func returnQueueDecision(request ReturnRequest) (QueueDecision, error) {
	d := QueueDecision{To: QueueFail, Reason: request.Failure.Reason, Message: request.Failure.Message, RecoveryComplete: true}
	switch request.To {
	case "", QueueFail:
	case QueueRetry:
		if !request.KeepPrevious {
			return d, fmt.Errorf("只有部分成员回送可以直接进入 Retry")
		}
		plan, err := PlanRetry(d.Reason, time.Time{})
		if err != nil {
			return d, err
		}
		d.To, d.RetryPlan, d.WaitingFor = QueueRetry, plan, string(plan.Condition)
	default:
		return d, fmt.Errorf("未知回送目标队列 %q", request.To)
	}
	return d, nil
}

// validateReturn 核对当前组身份和实际要重排的成员；已绑定 Pod 不能靠重新入队搬家。
func validateReturn(request ReturnRequest, view *group.View) error {
	source := request.Source
	key := source.Key().Group
	if source.State().Phase != UnitProcessing {
		return fmt.Errorf("只能回送 Buffer 正在处理的 Unit")
	}
	if view == nil || view.Group == nil || view.Group.Namespace != key.Namespace || view.Group.Name != key.Name || view.Group.UID != key.UID || view.Group.DeletionTimestamp != nil {
		return fmt.Errorf("原组已删除或重建，不能回送旧成员")
	}
	mode := SubmitMode(view.Group.Spec.SubmitMode)
	if mode == "" {
		mode = AllTogether
	}
	if mode != source.Mode() || (request.KeepPrevious && mode != AllowPartial) {
		return fmt.Errorf("回送模式与当前组声明不符")
	}
	original := make(map[PodRef]bool)
	for _, ref := range source.Members() {
		original[ref] = true
	}
	current := make(map[PodRef]*corev1.Pod)
	for _, member := range view.Members {
		if member.Group == key && member.Pod != nil {
			current[PodRef{Name: member.Pod.Name, UID: member.Pod.UID}] = member.Pod
		}
	}
	returned := make(map[PodRef]bool)
	for _, ref := range request.Members {
		pod := current[ref]
		if !original[ref] || returned[ref] || pod == nil {
			return fmt.Errorf("回送成员缺失、重复或不属于原 Unit: %s", ref.Name)
		}
		if pod.Spec.NodeName != "" || pod.DeletionTimestamp != nil || (pod.Status.Phase != "" && pod.Status.Phase != corev1.PodPending) {
			return fmt.Errorf("回送成员已绑定、删除中或已结束: %s", ref.Name)
		}
		returned[ref] = true
	}
	if mode == AllTogether && len(returned) != len(original) {
		return fmt.Errorf("Atomic 必须回送本轮全部成员")
	}
	if request.KeepPrevious && len(returned) == len(original) {
		return fmt.Errorf("部分回送需要给原 Unit 留下继续执行的成员")
	}
	return nil
}
