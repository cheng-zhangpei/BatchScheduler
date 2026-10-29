package executor

import (
	"errors"
	"fmt"

	"batchscheduler/src/group"
	"batchscheduler/src/scheduler"
	"batchscheduler/src/scheduler/resourcecache"
	"batchscheduler/src/scheduler/steps"
	"batchscheduler/src/workqueue"
)

type schedulingFailure struct{ info group.FailureInfo }

func (e *schedulingFailure) Error() string { return e.info.Message }

// noCandidateFailure 把实际过滤结果转成已有内部错误；候选缺失不等于程序执行出错。
func noCandidateFailure(members []workqueue.PodRef, results map[workqueue.PodRef]scheduler.PodFilterResult) error {
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
		return &schedulingFailure{info: group.FailureInfo{Reason: reason, Message: fmt.Sprintf("Pod %s 没有可行节点（%s）", ref.Name, reason)}}
	}
	return nil
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
	// Assume 复核的是选定计划。资源与端口暂时不满足时等待相关调度条件变化，节点消失则等节点变化。
	if errors.Is(err, resourcecache.ErrAssumeNoFit) {
		return &group.FailureInfo{Reason: workqueue.ReasonUnschedulable, Message: err.Error()}
	}
	if errors.Is(err, resourcecache.ErrAssumeNodeMissing) {
		return &group.FailureInfo{Reason: workqueue.ReasonNodeUnavailable, Message: err.Error()}
	}
	// 未确认是可重试错误时采用通用 SchedulingError，不凭错误字符串猜重试资格。
	return &group.FailureInfo{Reason: workqueue.ReasonSchedulingError, Message: err.Error()}
}
