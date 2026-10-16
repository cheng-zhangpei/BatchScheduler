package workqueue

import (
	"batchscheduler/src/group"
	"k8s.io/apimachinery/pkg/types"
)

// UnitKey 区分同一组的不同调度单元。ID 的生成方式留待后续确定。
// 成员范围改变或生成新一轮单元时，应使用新 ID。
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

// Unit 是本轮范围固定的 Pod 集合，可以覆盖整组、部分成员或单个 Pod。
// 实现应保证 Key、Mode 和成员范围在入队后不变，Members 不暴露可变内部切片。
// 此步只定义接口，暂不实现具体单元、ID 生成和从 View 拆分单元的策略。
type Unit interface {
	Key() UnitKey
	Members() []PodRef
	Mode() SubmitMode
}
