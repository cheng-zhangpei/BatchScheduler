// Package v1alpha1 定义本项目的组声明，与上游 PodGroup API 分开。
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const GroupName = "scheduling.batchscheduler.io"

// 我们需要定义一个资源的标识符
var (
	GroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1alpha1"}
	PodGroupGVR  = GroupVersion.WithResource("podgroups")
)

// PodGroup 声明组，operator 负责创建成员 Pod，调度器只观察此声明。
// 描述成员数、提交模式和组放置偏好；不在这里加入 Pod 模板或运行状态机。
type PodGroup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              PodGroupSpec `json:"spec"`
}

type PodGroupSpec struct {
	// Members 是期望的成员总数，必须大于零；暂不引入独立的 minMember。
	Members int32 `json:"members"`
	// SubmitMode 声明整组提交或允许部分提交，缺省按 AllTogether 解释。
	SubmitMode string `json:"submitMode,omitempty"`
	// Placement 是用户声明的组放置偏好；省略时不增加通信或资源整合要求。
	Placement *PlacementSpec `json:"placement,omitempty"`
}

func (s PodGroupSpec) DeepCopy() *PodGroupSpec {
	s.Placement = s.Placement.DeepCopy()
	return &s
}
