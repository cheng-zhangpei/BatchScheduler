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
// 当前仅描述固定成员数，不提前加入模板、重试策略或运行状态机。
type PodGroup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              PodGroupSpec `json:"spec"`
}

type PodGroupSpec struct {
	// Members 是期望的成员总数，必须大于零；暂不引入独立的 minMember。
	Members int32 `json:"members"`
}
