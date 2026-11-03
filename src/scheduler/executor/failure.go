package executor

import (
	"errors"
	"fmt"

	"batchscheduler/src/group"
	"batchscheduler/src/scheduler"
	"batchscheduler/src/scheduler/planner"
	"batchscheduler/src/scheduler/resourcecache"
	"batchscheduler/src/scheduler/steps"
	"batchscheduler/src/workqueue"
)

type schedulingFailure struct{ info group.FailureInfo }

func (e *schedulingFailure) Error() string { return e.info.Message }

// unplacedAfterFilter 收集 Partial 中没有候选的成员。需要整组放置时，遇到第一个失败就结束。
func unplacedAfterFilter(members []workqueue.PodRef, results map[workqueue.PodRef]scheduler.PodFilterResult, allowPartial bool) ([]planner.UnplacedMember, error) {
	var unplaced []planner.UnplacedMember
	for _, ref := range members {
		result := results[ref]
		if len(result.Candidates) != 0 {
			continue
		}
		reason, plugin, mixed := workqueue.ReasonUnschedulable, "", false
		for _, rejection := range result.Rejected {
			if plugin == "" {
				plugin = rejection.Plugin
			} else if plugin != rejection.Plugin {
				mixed = true
			}
		}
		if !mixed {
			switch plugin {
			case "":
				reason = workqueue.ReasonNodeUnavailable
			case "NodeResourcesFit":
				reason = workqueue.ReasonInsufficientResources
			case "NodeUnschedulable":
				reason = workqueue.ReasonNodeUnavailable
			case "NodeAffinity", "NodeName", "TaintToleration", "NodePorts":
				reason = workqueue.ReasonPlacementConstraints
			}
		}
		failure := group.FailureInfo{Reason: reason, Message: fmt.Sprintf("Pod %s 没有可行节点（%s）", ref.Name, reason)}
		if !allowPartial {
			return nil, &schedulingFailure{info: failure}
		}
		unplaced = append(unplaced, planner.UnplacedMember{Pod: ref, Failure: failure})
	}
	if len(unplaced) != 0 && len(unplaced) == len(members) {
		// 没有成员能进入评分，整轮沿用第一个失败原因回送。
		return nil, &schedulingFailure{info: unplaced[0].Failure}
	}
	return unplaced, nil
}

func executionFailure(err error) *group.FailureInfo {
	var binding *steps.BindingError
	if errors.As(err, &binding) {
		return &group.FailureInfo{Reason: binding.Reason, Message: binding.Error()}
	}
	var known *schedulingFailure
	if errors.As(err, &known) {
		failure := known.info
		return &failure
	}
	// 搜索没有找到方案，或 Assume 复核时方案已放不下，都等待相关调度条件变化。
	if errors.Is(err, planner.ErrNoFeasiblePlan) || errors.Is(err, resourcecache.ErrAssumeNoFit) {
		return &group.FailureInfo{Reason: workqueue.ReasonUnschedulable, Message: err.Error()}
	}
	if errors.Is(err, resourcecache.ErrAssumeNodeMissing) {
		return &group.FailureInfo{Reason: workqueue.ReasonNodeUnavailable, Message: err.Error()}
	}
	// 未确认是可重试错误时采用通用 SchedulingError，不凭错误字符串猜重试资格。
	return &group.FailureInfo{Reason: workqueue.ReasonSchedulingError, Message: err.Error()}
}
