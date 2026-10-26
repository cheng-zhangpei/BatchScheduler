package workqueue

import (
	"fmt"

	"batchscheduler/src/group"
)

// Definition 返回独立副本，插件或执行器修改参数不会改掉队列中冻结的声明。
func (u *schedulingUnit) Definition() *group.Definition {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.definition.DeepCopy()
}

func (u *schedulingUnit) SetDefinition(definition *group.Definition) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.state.Phase != UnitNew {
		return fmt.Errorf("%w: 组声明也已冻结", ErrUnitFrozen)
	}
	if definition == nil || definition.Spec.Members <= 0 {
		return fmt.Errorf("Unit 缺少有效的组声明")
	}
	mode := SubmitMode(definition.Spec.SubmitMode)
	if mode == "" {
		mode = AllTogether
	}
	if mode != u.mode {
		return fmt.Errorf("Unit 的提交模式与组声明不一致")
	}
	u.definition = definition.DeepCopy()
	return nil
}
