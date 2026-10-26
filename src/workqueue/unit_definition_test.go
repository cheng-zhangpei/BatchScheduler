package workqueue

import (
	"errors"
	"testing"

	scheduling "batchscheduler/src/apis/scheduling/v1alpha1"
	"batchscheduler/src/group"
)

func TestSelectFreezesIndependentGroupDefinition(t *testing.T) {
	view := testView("work", "job", AllTogether, 2, 2)
	view.Group.Generation = 7
	view.Group.Spec.Placement = &scheduling.PlacementSpec{Communication: []scheduling.CommunicationLink{{Name: "ab", From: "a", To: "b", Weight: 10}}}
	w := queueFor(view)
	selectOK(t, w, SelectRequest{View: view})
	u := drainActive(t, w, 1)[0]
	view.Group.Spec.Placement.Communication[0].Weight = 99
	definition := u.Definition()
	if definition.Generation != 7 || definition.Spec.Placement.Communication[0].Weight != 10 {
		t.Fatal("入队后原声明的修改影响了冻结 Unit")
	}
	definition.Spec.Placement.Communication[0].Weight = 88
	if u.Definition().Spec.Placement.Communication[0].Weight != 10 {
		t.Fatal("查询返回了可改写 Unit 的共享声明")
	}
	if err := u.SetDefinition(group.DefinitionFor(view.Group)); !errors.Is(err, ErrUnitFrozen) {
		t.Fatal("冻结后不能替换声明")
	}
}
