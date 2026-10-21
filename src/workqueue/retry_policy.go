package workqueue

import (
	"fmt"
	"time"
)

// 这些是本项目的内部失败分类，不是原生 Pod Reason，也不解析 Message 猜错误。
// 未来由调度周期/感知机把已确认的结果归类，再写入 Failure.Reason。
const (
	// ReasonUnschedulable 原生状态只说明暂时调度不了，不能仅凭它区分资源、节点或放置约束。
	ReasonUnschedulable = "Unschedulable"
	// ReasonInsufficientResources 等资源释放、节点加入或容量变化；相关事件仅说明值得再试，不保证能放置。
	ReasonInsufficientResources = "InsufficientResources"
	// ReasonNodeUnavailable 等节点恢复、解除不可调度状态或其他相关节点条件变化。
	ReasonNodeUnavailable = "NodeUnavailable"
	// ReasonPlacementConstraints 等节点标签、污点、拓扑等相关条件变化；不是靠时间自动修复声明。
	ReasonPlacementConstraints = "PlacementConstraints"
	// ReasonDependencyNotReady 等外部前置依赖可用，例如调度所需的存储相关对象；具体事件后续定义。
	ReasonDependencyNotReady = "DependencyNotReady"
	// ReasonTransientSchedulingError 调度阶段的临时内部错误，例如读取临时失败；必须确定没有未闭合的绑定请求。
	ReasonTransientSchedulingError = "TransientSchedulingError"
	// ReasonBindingRejected 已确认可重试的临时绑定拒绝，且成员仍未绑定并完成清理。
	// 权限/无效请求须归入下方对应分类；请求超时不能直接归为此类。
	ReasonBindingRejected = "BindingRejected"
)

// 下列分类不进入自动 Retry；未知分类同样默认拒绝，而不是“所有错误都重试”。
const (
	ReasonSchedulingError = "SchedulingError" // 原生 SchedulerError 未证明是临时故障，先留在 Fail 等进一步处理。
	ReasonBindingUnknown  = "BindingUnknown"  // 留给缓冲区确认结果，不能重复绑定。
	ReasonInvalidSpec     = "InvalidSpec"     // 修改声明或由 operator 重建后重新评估。
	ReasonForbidden       = "Forbidden"       // 修复权限后显式重新评估，不定时空转。
	ReasonObjectDeleted   = "ObjectDeleted"   // 清理旧身份；新 UID 是新对象。
	ReasonRecoveryPending = "RecoveryPending" // 先完成清理/恢复，再重新分类。
)

type RetryCondition string

const (
	RetryOnScheduling RetryCondition = "SchedulingConditionsChanged" // 任一相关资源/节点/放置变化值得再试。
	RetryOnResources  RetryCondition = "ResourcesChanged"
	RetryOnNodes      RetryCondition = "NodeConditionsChanged"
	RetryOnPlacement  RetryCondition = "PlacementConditionsChanged"
	RetryOnDependency RetryCondition = "DependencyChanged"
	RetryOnBackoff    RetryCondition = "BackoffElapsed"
)

// Matches 判断收到的变化是否属于这个等待条件；通用 Unschedulable 接受三种相关变化。
func (condition RetryCondition) Matches(changed RetryCondition) bool {
	if condition == RetryOnScheduling {
		return changed == RetryOnScheduling || changed == RetryOnResources || changed == RetryOnNodes || changed == RetryOnPlacement
	}
	return changed != "" && condition == changed
}

// RetryPlan 记录 Retry → Active 的门槛；事件监听和定时器由外部入口负责。
//   - 事件型：相关变化提示本单元值得再试，并且 NotBefore（如有）已到。
//   - 时间型：NotBefore 必填，到期后可以再次尝试。
//
// 激活前都须重查组/Pod UID、删除状态、单元归属，以及没有新的绑定中/未知结果。
// “条件可能改善”不是调度成功证明，不在这里提前做一次节点放置算法。
type RetryPlan struct {
	Condition RetryCondition
	NotBefore time.Time
}

func planRetry(reason string, notBefore time.Time) (RetryPlan, error) {
	plan := RetryPlan{NotBefore: notBefore}
	switch reason {
	case ReasonUnschedulable:
		plan.Condition = RetryOnScheduling
	case ReasonInsufficientResources:
		plan.Condition = RetryOnResources
	case ReasonNodeUnavailable:
		plan.Condition = RetryOnNodes
	case ReasonPlacementConstraints:
		plan.Condition = RetryOnPlacement
	case ReasonDependencyNotReady:
		plan.Condition = RetryOnDependency
	case ReasonTransientSchedulingError, ReasonBindingRejected:
		if notBefore.IsZero() {
			return RetryPlan{}, fmt.Errorf("临时错误 %s 必须指定 RetryAfter 退避截止时间", reason)
		}
		plan.Condition = RetryOnBackoff
	default:
		return RetryPlan{}, fmt.Errorf("失败类型 %q 不支持自动重试，需先确认或人工/控制器处理", reason)
	}
	return plan, nil
}
