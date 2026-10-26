package group

import scheduling "batchscheduler/src/apis/scheduling/v1alpha1"

// Definition 保存本轮使用的组声明和声明版本；Spec 直接使用 API 类型，不另抄一套字段。
// 它不保存 Pod 或执行进度。Unit 冻结后，这份声明也不再改变。
type Definition struct {
	Generation int64
	Spec       scheduling.PodGroupSpec
}

func DefinitionFor(g *scheduling.PodGroup) *Definition {
	return &Definition{Generation: g.Generation, Spec: *g.Spec.DeepCopy()}
}

func (d *Definition) DeepCopy() *Definition {
	if d == nil {
		return nil
	}
	return &Definition{Generation: d.Generation, Spec: *d.Spec.DeepCopy()}
}
