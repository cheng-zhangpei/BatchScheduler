package staging

import (
	"batchscheduler/src/group"
	"batchscheduler/src/workqueue"
	corev1 "k8s.io/api/core/v1"
)

// Observation 表示“这个组可能变了，请再看一遍”。当前实际只使用 Group。
// Pod、View 暂时只是预留字段；Observe 会重新读缓存，不直接采用传入的旧对象。
// 三种标准调度原因已在 Observe 中映射；TODO：继续补充其他明确的原生原因。
// 容器崩溃不能直接当成调度失败。
type Observation struct {
	Group group.Key
	Pod   *workqueue.PodRef // nil 表示组级观察；非 nil 不代表一定需要重调度。
	View  *group.View
}

// Operation 表示让执行器做哪件事：算节点、预占资源、绑定、恢复。
type Operation string

const (
	ActionSelect      Action    = "Select"     // 组织未被 Center 持有的成员，交给首次入队入口。
	ActionTransition  Action    = "Transition" // 对队列持有的 Unit 执行迁移，不借用 Center 的 Record。
	OperationSchedule Operation = "Schedule"
	OperationAssume   Operation = "Assume"
	OperationBind     Operation = "Bind"
	OperationRecover  Operation = "Recover"
)

// Feedback 是执行器返回的结果，例如“action-3 这次绑定成功了”。
// 当前必须汇总整个操作再上报（Pod=nil），一个成员成功不能代表整个操作成功。
// Unknown=true 表示还不知道成败；Unknown=false 且 Failure=nil 才表示这个操作成功。
type Feedback struct {
	Unit                    workqueue.UnitKey
	ActionID                string
	Operation               Operation
	Pod                     *workqueue.PodRef
	Unknown                 bool
	Failure                 *group.FailureInfo
	Plan                    map[workqueue.PodRef]string // Schedule 成功时提交完整成员 → 节点计划。
	ResponsibilitiesSettled bool                        // Bind/Recover 已收尾：没有还在跑的请求，资源预占也已释放或转成正式占用。
}

type Action string

const (
	ActionWait    Action = "Wait"    // 等待观察或执行反馈。
	ActionExecute Action = "Execute" // 派发 Operation，等待反馈。
	ActionReturn  Action = "Return"  // 经 Select 回送；必要时先组织下一轮 Unit。
	ActionRelease Action = "Release" // 已满足释放条件，结束本轮记录。
)

// Decision 是 Evaluate 写给 Apply 的下一步安排，本身不表示已经执行。
// Select 没有指定 Unit；Execute/Return/Release 使用 Record.Revision 检查是否过期。
// Transition 使用 Transition.ExpectedRevision，即队列中 Unit 的版本，不用 Record 的版本。
type Decision struct {
	Transition *workqueue.TransitionRequest // 仅迁移队列时使用：从哪里搬到哪里、依据哪个队列版本。
	Group      group.Key
	Unit       *workqueue.UnitKey
	Revision   uint64
	Action     Action
	Operation  Operation // 仅 ActionExecute 使用。
	Reason     string
	To         workqueue.QueueName // ActionReturn 的建议目标；实际首次入队仍由 Select 决定。
	Members    []workqueue.PodRef  // 这次要处理的成员；Atomic 取整轮，Partial 能定位错误时取失败成员。
	Failure    *group.FailureInfo
}

// Record 记录一个 Unit 这一轮做到哪了。Members 是我们自己的进度，Observed 是集群实际情况。
// 它由 Accept 创建，本轮结束后删除，不保存 Group 的完整历史。
// PendingAction 为空只说明当前没在等某个操作，不代表已经完成所有收尾。
type Record struct {
	Unit          workqueue.Unit
	Members       map[workqueue.PodRef]group.PodState
	PendingAction string         // 正在等哪个操作，例如 action-3；空字符串表示没有。
	Observed      *ObservedGroup // 最近一次从 informer 缓存读到的成员情况；nil 表示还没读过。
	Revision      uint64         // 这份 Record 改过几次；旧 Decision 不能直接用在新版记录上。
	Generation    int64          // Accept 时组声明的版本，用来发现之后的声明修改。
	// Failure 是执行器经 Report 确认的本轮失败，不从 Unit 的历史失败中照搬。
	// informer 的匹配结果保存在 Observed.Members[ref].Failure，不会覆盖这里。
	Failure                 *group.FailureInfo
	RecoveryComplete        bool                        // 执行器已确认必要的恢复步骤完成，不是靠 Observe 猜出来的。
	ResponsibilitiesSettled bool                        // 执行请求和资源预占都已收尾；它为 true 才可能删除本轮记录。
	NextOperation           Operation                   // 上一步成功后，下一步准备做什么。
	Plan                    map[workqueue.PodRef]string // 调度算法算出的 Pod → Node 计划。
	PendingOperation        Operation                   // 正在等待的具体操作，例如 Bind。
	PendingDecision         Action                      // 正在等待的动作类型，例如 Execute 或 Return。
	LastFeedback            *Feedback                   // 只记住最近一次结果；相同结果再发来时不重复处理。
}

// ObservedGroup 保存“这个 Unit 的成员在集群中是什么样”，不会复制组内所有无关 Pod。
// Exists=false 表示没找到原来那个组；它的旧 Pod 可能还在，不能直接当作已删除。
type ObservedGroup struct {
	Exists     bool
	Deleting   bool
	Generation int64
	Members    map[workqueue.PodRef]ObservedPod
}

type ObservedPod struct {
	Failure *group.FailureInfo // Observe 从 Pod 调度条件匹配出的内部错误；与执行器的 Record.Failure 分开保存。
	// Known=false：原来的组查不到了，暂时无法从组视图确认这个 Pod 的情况。
	// Known=true、Present=false：组还在，但里面已经没有这个名称和 UID 的成员。
	Known    bool
	Present  bool
	Deleting bool
	NodeName string
	Status   corev1.PodStatus // 复制 Pod 状态供后面分析；当前还没有完整错误映射。
}
