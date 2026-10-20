package group

// FailureInfo 记录可解释的错误；Reason 用于稳定分类，Message 用于具体诊断。
// 这份记录不是“错误仍然存在”的证明，后续判断须结合当前对象和尝试结果。
type FailureInfo struct {
	Reason  string
	Message string
}

// PodSchedulingPhase 是调度器视角，不替代 Pod.status.phase。
// Pending、Running、Failed、容器退出原因等事实仍从 PodView.Pod 中读取。
type PodSchedulingPhase string

const (
	PodSchedulingUnknown PodSchedulingPhase = ""               // 尚未建立观察与内部记录。
	PodUnscheduled       PodSchedulingPhase = "Unscheduled"    // 未绑定，且没有本地预留或绑定操作。
	PodReserved          PodSchedulingPhase = "Reserved"       // 已有本地候选节点和临时占用。
	PodBinding           PodSchedulingPhase = "Binding"        // 正在提交绑定。
	PodBindingUnknown    PodSchedulingPhase = "BindingUnknown" // 请求结果不确定，需要观察 API 确认。
	PodBound             PodSchedulingPhase = "Bound"          // 已观察到节点分配，不代表容器运行成功。
)

// PodState 是单个 Pod UID 对应的内部记录，不写入原生 Pod.status。
// 未来按 Pod UID 管理，不能让同名重建的 Pod 继承旧记录。
type PodState struct {
	Phase PodSchedulingPhase
	// UnitID 表示当前所属单元，其组身份沿用 PodView.Group。
	// 空值表示尚未归入单元；分配及解除规则尚未实现。
	UnitID      string
	PlannedNode string // 本地计划，不冒充 API 中的 spec.nodeName。
	LastFailure *FailureInfo
}

// GroupPhase 概括组在调度层的进展，不表示某个队列的位置或业务计算结果。
// 同时存在多种成员状态时如何聚合，留待 Select 规则确定。
type GroupPhase string

const (
	GroupUnknown        GroupPhase = ""
	GroupWaiting        GroupPhase = "Waiting"        // 前置条件尚未满足，具体条件另记。
	GroupReady          GroupPhase = "Ready"          // 成员具备尝试条件，不保证节点资源可放置。
	GroupScheduling     GroupPhase = "Scheduling"     // 有本轮单元正在规划或提交绑定。
	GroupPartiallyBound GroupPhase = "PartiallyBound" // 已观察到部分成员绑定。
	GroupBound          GroupPhase = "Bound"          // 已满足约定的组绑定条件，不代表作业完成。
	GroupRecovering     GroupPhase = "Recovering"     // 正在确认结果、清理或确定下一轮范围。
	GroupDeleting       GroupPhase = "Deleting"       // 组删除中，不再发起新调度。
)

// State 是按 group.Key 管理的组内部状态，不是 PodGroup CR 的 status。
// 成员数和已绑定数量可从视图计算，暂不额外存一套计数。
type State struct {
	Phase              GroupPhase
	ObservedGeneration int64 // 这份状态对应的组声明版本。
	WaitingFor         string
	LastFailure        *FailureInfo
	// UnitIDs 记录本组持有的单元 ID，包括组装中和已冻结的单元。
	// 完整键由当前 group.Key + ID 组成，避免 group 与 workqueue 相互依赖。
	// 单元本体和处理状态由 workqueue 管理；登记、清理和组状态聚合尚未实现。
	UnitIDs map[string]struct{}
}
