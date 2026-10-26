package staging

import (
	"context"
	"errors"

	"batchscheduler/src/group"
	"batchscheduler/src/workqueue"
)

var (
	ErrStaleDecision      = errors.New("评估决策已过期，需重新评估")
	ErrHandlerUnavailable = errors.New("动作适配器尚未接入")
	ErrHandoffUnknown     = errors.New("交接结果未知，须确认后再释放")
	// ErrExecutionNotStarted 表示执行器确认没有预占或绑定操作在途，可以取消本次 Pending。
	// 用于请求未被接收；已接收后的进度、失败、待实现步骤通过 Report 返回。
	ErrExecutionNotStarted = errors.New("尚未开始资源操作")
)

// Handlers 保存外部函数，NewBuffer 时传进来，启动后不要更换。
// “注入”只表示给字段赋一个函数；调用这些字段不会自动新建 goroutine。
type Handlers struct {
	// QueueSnapshot 查询指定组在 Fail、Retry 里的 Unit；只读，不取出任务。
	QueueSnapshot func(group.Key) []workqueue.QueuedUnit
	// WaitingGroups 查询哪些组在 Fail、Retry 中还有任务，供定时检查和事件通知使用。
	WaitingGroups func() []group.Key
	// Transition 调用 WorkQueue.Transition，真正移动 Fail/Retry 中的任务。
	Transition func(context.Context, workqueue.TransitionRequest) error
	// Execute 接收整轮执行或独立恢复请求，进展和结束由执行器调用 Report。
	// 一般 error 保留 Pending；ErrExecutionNotStarted 明确没有资源操作，则取消 Pending。
	Execute func(context.Context, Execution) error
	// SelectGroup 调用 WorkQueue.Select，判断是否需要创建 Unit 并首次入队。
	SelectGroup func(context.Context, *group.View) error
	// Handoff 在旧 Unit 结束时处理队列里的旧关联，失败回送时还要接住下一轮任务。
	// 返回 nil 才表示这些事已做完；普通 error 必须表示完全没做成，才能安全重试。
	// 如果不知道做成没有，返回 ErrHandoffUnknown，等查清后再调用 ConfirmHandoff。
	// Release 已接到 WorkQueue.ReleaseUnit；Return 已接到 WorkQueue.Select 的回送输入。
	Handoff func(context.Context, Handoff) error
}

// Execution 是发给执行器的一张任务单，说明本次操作的编号、成员和节点计划。
type Execution struct {
	ActionID   string
	Unit       workqueue.UnitKey
	Operation  Operation
	Mode       workqueue.SubmitMode // 从冻结 Unit 透传，后续组评分读取它。
	Definition *group.Definition    // 本轮冻结的组声明；不从最新 CR 临时替换。
	Members    []workqueue.PodRef
	Plan       map[workqueue.PodRef]string
	Failure    *group.FailureInfo
}

// Handoff 是发给队列处理函数的结束通知：哪个旧 Unit，要回送还是只结束。
// Partial 即使只重试部分成员，也要明确处理旧 Unit 其他成员的记录，不能直接丢掉。
type Handoff struct {
	ActionID string
	Unit     workqueue.Unit
	Decision Decision
}
