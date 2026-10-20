package workqueue

import (
	"errors"
	"fmt"
	"sync"

	"batchscheduler/src/group"
	"k8s.io/apimachinery/pkg/types"
)

// UnitKey 区分同一组的不同调度单元。当前 ID 使用进程内递增序号，不跨重启持久化。
// 组装期间追加成员不改变 ID；冻结后若需改变范围或生成新一轮单元，应使用新 ID。
type UnitKey struct {
	Group group.Key
	ID    string
}

// PodRef 引用本轮成员，namespace 沿用 UnitKey.Group.Namespace。
// UID 避免同名 Pod 重建后被当作本轮原成员。
type PodRef struct {
	Name string
	UID  types.UID
}

type SubmitMode string

const (
	// AllTogether 要求本轮成员整体推进，具体失败处理规则尚未定义。
	AllTogether SubmitMode = "AllTogether"
	// AllowPartial 允许本轮部分成员先推进。
	AllowPartial SubmitMode = "AllowPartial"
)

var ErrUnitFrozen = errors.New("调度单元已冻结，不能追加成员")

// Unit 是同一组内的弹性 Pod 集合，持有成员和本轮生命周期状态。
// 组装阶段可追加成员；提交入队前冻结，后续处理和重试都不能改变成员。
// 跨单元的 Pod 归属校验和组内登记由 Select 的组装入口处理。
type Unit interface {
	Key() UnitKey
	Mode() SubmitMode
	Members() []PodRef
	State() UnitState
	Frozen() bool
	AddMember(PodRef) error
	RemoveMember(PodRef) error
	Freeze() error
	// MarkQueued 仅登记首次入队状态，实际 Add/Put 由 Select 完成。
	MarkQueued(QueueName, *group.FailureInfo, string) error
	// MarkRetry 只登记 Fail → Retry 状态；队列条目由 Transition 协调。
	MarkRetry(RetryPlan) error
	// MarkActive 只登记 Retry → Active，实际搬移由 Transition 完成。
	MarkActive() error
	// MarkProcessing / MarkFinished 登记暂存区接收与完成；不操作实际队列条目。
	MarkProcessing() error
	MarkFinished() error
}

// schedulingUnit 是默认实现，同一个对象贯穿组装、冻结及后续处理。
type schedulingUnit struct {
	mu      sync.RWMutex
	key     UnitKey
	mode    SubmitMode
	members []PodRef
	state   UnitState
}

var _ Unit = (*schedulingUnit)(nil)

// NewUnit 创建空的组装中单元；调用方负责提供本组内唯一的 ID。
func NewUnit(key UnitKey, mode SubmitMode) Unit {
	return &schedulingUnit{key: key, mode: mode, state: UnitState{Phase: UnitNew, Queue: QueueNone}}
}

func (u *schedulingUnit) Key() UnitKey { return u.key }

func (u *schedulingUnit) Mode() SubmitMode { return u.mode }

// State 返回副本，避免外部绕过生命周期入口修改内部记录。
func (u *schedulingUnit) State() UnitState {
	u.mu.RLock()
	defer u.mu.RUnlock()
	state := u.state
	if state.LastFailure != nil {
		failure := *state.LastFailure
		state.LastFailure = &failure
	}
	if state.RetryAfter != nil {
		retryAfter := *state.RetryAfter
		state.RetryAfter = &retryAfter
	}
	return state
}

func (u *schedulingUnit) Members() []PodRef {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return append([]PodRef(nil), u.members...)
}

func (u *schedulingUnit) Frozen() bool {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.state.Phase != UnitNew
}

// AddMember 只允许在组装期间追加；相同引用重复到达时不重复添加。
// PodRef 不含 ownerReference，调用方须先确认 Pod 属于本组。
func (u *schedulingUnit) AddMember(pod PodRef) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.state.Phase != UnitNew {
		return ErrUnitFrozen
	}
	if pod.Name == "" || pod.UID == "" {
		return fmt.Errorf("单元成员必须具有 Pod 名称和 UID")
	}
	for _, member := range u.members {
		if member == pod {
			return nil
		}
		if member.Name == pod.Name || member.UID == pod.UID {
			return fmt.Errorf("单元内 Pod 名称或 UID 冲突: %s", pod.Name)
		}
	}
	u.members = append(u.members, pod)
	return nil
}

// RemoveMember 仅在组装阶段移除精确引用，避免同名新 Pod 误删旧尝试的成员。
// 重复移除安全；冻结后与追加一样被禁止。
func (u *schedulingUnit) RemoveMember(pod PodRef) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.state.Phase != UnitNew {
		return ErrUnitFrozen
	}
	for i, member := range u.members {
		if member == pod {
			copy(u.members[i:], u.members[i+1:])
			u.members[len(u.members)-1] = PodRef{}
			u.members = u.members[:len(u.members)-1]
			break
		}
	}
	return nil
}

// Freeze 将组装中的单元转为冻结状态，重复调用安全，不执行入队。
// 是否已满足整组/部分提交条件，由调用方在冻结前判断。
func (u *schedulingUnit) Freeze() error {
	u.mu.Lock()
	defer u.mu.Unlock()
	if len(u.members) == 0 {
		return fmt.Errorf("不能冻结空调度单元")
	}
	if u.state.Phase == UnitNew {
		u.state.Phase = UnitFrozen
	}
	return nil
}

// MarkQueued 只接受已冻结且从未入队的单元，避免首次入队覆盖已有位置。
// 调用方必须紧接着发布到对应队列；后续队列间迁移不使用此入口。
func (u *schedulingUnit) MarkQueued(queue QueueName, failure *group.FailureInfo, waitingFor string) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	if !isQueue(queue) {
		return fmt.Errorf("无效的入队位置: %q", queue)
	}
	if u.state.Phase != UnitFrozen || u.state.Queue != QueueNone {
		return fmt.Errorf("只有已冻结且尚未入队的单元可以首次入队")
	}
	u.state.Phase = UnitQueued
	u.state.Queue = queue
	u.state.WaitingFor = waitingFor
	u.state.LastFailure = nil
	if failure != nil {
		copy := *failure
		u.state.LastFailure = &copy
	}
	return nil
}

func (u *schedulingUnit) MarkRetry(plan RetryPlan) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.state.Phase != UnitQueued || u.state.Queue != QueueFail {
		return fmt.Errorf("只有 Fail 中的单元可以迁入 Retry")
	}
	u.state.Queue = QueueRetry
	u.state.WaitingFor = string(plan.Condition)
	u.state.RetryAfter = nil
	if !plan.NotBefore.IsZero() {
		deadline := plan.NotBefore
		u.state.RetryAfter = &deadline
	}
	// LastFailure 保留用于解释本次等待；成员范围和冻结状态不变。
	return nil
}

func (u *schedulingUnit) MarkActive() error {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.state.Phase != UnitQueued || u.state.Queue != QueueRetry {
		return fmt.Errorf("只有 Retry 中的单元可以重新进入 Active")
	}
	u.state.Queue = QueueActive
	u.state.WaitingFor = ""
	u.state.RetryAfter = nil
	// 历史失败仍保留作诊断，不把它当成本轮调度结果。
	return nil
}

func (u *schedulingUnit) MarkProcessing() error {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.state.Phase != UnitQueued || (u.state.Queue != QueueActive && u.state.Queue != QueueFastRetry) {
		return fmt.Errorf("只有 Active/FastRetry 出队单元可以进入处理阶段")
	}
	u.state.Phase, u.state.Queue = UnitProcessing, QueueNone
	return nil
}

func (u *schedulingUnit) MarkFinished() error {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.state.Phase != UnitProcessing {
		return fmt.Errorf("只有处理中的单元可以确认本轮结束")
	}
	u.state.Phase, u.state.Queue = UnitDone, QueueNone
	return nil
}
