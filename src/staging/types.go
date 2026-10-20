package staging

import (
	"batchscheduler/src/group"
	"batchscheduler/src/workqueue"
	corev1 "k8s.io/api/core/v1"
)

// Observation 是独立的集群观察输入。
// Group 是变化提示的精确身份，Pod 可进一步指出触发成员，包括删除的旧 UID。
// Observe 会重新读 Store，View 只保留为来源上下文，不作为覆盖当前事实的依据。
// 因此迟到的删除事件不会覆盖当前缓存；同名新 Group 也不会冒充旧 UID。
// TODO：原生错误到内部失败结果的映射，由后续 Evaluate/感知规则实现。
// 不得长期持有 informer 缓存对象并修改它们；需要保留时复制必要字段。
type Observation struct {
	Group group.Key
	Pod   *workqueue.PodRef // nil 表示组级观察；非 nil 不代表一定需要重调度。
	View  *group.View
}

// Operation 表示交给执行器的工作阶段，不表示已经执行成功。
type Operation string

const (
	OperationSchedule Operation = "Schedule"
	OperationAssume   Operation = "Assume"
	OperationBind     Operation = "Bind"
	OperationRecover  Operation = "Recover"
)

// Feedback 只对应本地已登记的执行操作，区别于无需本地记录的 Observation。
// ActionID 区分同一 Unit 的不同操作，拒绝旧回调覆盖后续阶段；生成规则待实现。
// Pod 为 nil 时反馈针对整个操作；非 nil 时针对其中一个成员。
// Unknown 表示结果待确认；Failure 为空且 Unknown 为 false 表示该操作成功，
// 不意味着整个 Unit 已完成。未知结果即使有诊断错误，也不能按确认失败处理。
type Feedback struct {
	Unit      workqueue.UnitKey
	ActionID  string
	Operation Operation
	Pod       *workqueue.PodRef
	Unknown   bool
	Failure   *group.FailureInfo
}

type Action string

const (
	ActionWait    Action = "Wait"    // 等待观察或执行反馈。
	ActionExecute Action = "Execute" // 派发 Operation，等待反馈。
	ActionReturn  Action = "Return"  // 经 Select 回送；必要时先组织下一轮 Unit。
	ActionRelease Action = "Release" // 已满足释放条件，结束本轮记录。
)

// Decision 是行为意图；Apply 必须重查依据，不能把历史决策直接作为执行授权。
// Unit 可为空：观察入口可能在没有本地执行记录时决定组织新一轮任务。
// Unit 非空时 Revision 是本轮 Record 的版本，不是 Kubernetes resourceVersion；
// 变化后旧决策须重新评估。组级 Unit=nil 决策不保留版本，执行时须重读 Store。
// Apply 的实际版本核对、动作载荷和幂等执行留到下一步补齐。
type Decision struct {
	Group     group.Key
	Unit      *workqueue.UnitKey
	Revision  uint64
	Action    Action
	Operation Operation // 仅 ActionExecute 使用。
	Reason    string
	To        workqueue.QueueName // ActionReturn 的建议目标；实际首次入队仍由 Select 决定。
	Members   []workqueue.PodRef  // 本次恢复/回送范围；Atomic 为整轮，Partial 为失败子集。
	Failure   *group.FailureInfo
}

// Record 是缓冲区的一条本轮执行记录；按 UnitKey 保存，按 Group/Pod UID 建索引。
// 与长期 Group 资源不同，它只持有尚未结清的本轮责任，不复制整份 informer 缓存。
// Unit 成员已冻结；未来 compaction 必须创建新 Unit，不能修改本轮在途成员。
//
// Buffer 已实现登记、索引和锁；容量限制、超时与释放逻辑尚未实现。
// PendingAction 表示等待结果的操作；具体资源预留及交接凭据待资源缓存设计时补充，
// 不能仅凭 PendingAction 为空就判定所有责任已结清并删除记录。
type Record struct {
	Unit          workqueue.Unit
	Members       map[workqueue.PodRef]group.PodState
	PendingAction string
	Observed      *ObservedGroup // nil 表示尚未观察；不代替 Members 的内部执行状态。
	Revision      uint64         // 本轮记录版本；观察或执行状态改变后，旧决策需要重算。
	Generation    int64          // 接收时的组声明版本；声明变化后不能继续执行旧计划。
	// Failure 仅表示本轮已确认的执行失败，不从 Unit.LastFailure 历史记录复制。
	Failure *group.FailureInfo
	// RecoveryComplete 和 ResponsibilitiesSettled 必须由后续执行/恢复反馈明确登记。
	// 前者说明所需恢复完成；后者说明无未知操作、预留已清理/交接且执行器已结束。
	// 两者缺省 false，不能凭 Pod 已绑定、PendingAction 为空就假设资源账目已结清。
	RecoveryComplete        bool
	ResponsibilitiesSettled bool
}

// ObservedGroup 只保存本轮成员所需事实，不长期复制整个组及其所有 Pod。
// Exists=false 包括组删除或同名组已换 UID；不能据此断定原 Pod 已删除。
type ObservedGroup struct {
	Exists     bool
	Deleting   bool
	Generation int64
	Members    map[workqueue.PodRef]ObservedPod
}

type ObservedPod struct {
	// Known=false：当前没有匹配 UID 的组视图，不能确认成员是否仍存在。
	// Known=true、Present=false：精确成员已不在当前组视图，可能删除、重建或改归属。
	Known    bool
	Present  bool
	Deleting bool
	NodeName string
	Status   corev1.PodStatus // 深拷贝，保留条件/容器错误供后续解释，不直接等同调度失败。
}
