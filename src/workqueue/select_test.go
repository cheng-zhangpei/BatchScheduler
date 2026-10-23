package workqueue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	scheduling "batchscheduler/src/apis/scheduling/v1alpha1"
	"batchscheduler/src/group"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

type testViews map[string]*group.View

func (v testViews) Get(namespace, name string) (*group.View, error) {
	if view := v[namespace+"/"+name]; view != nil {
		return view, nil
	}
	return nil, apierrors.NewNotFound(scheduling.PodGroupGVR.GroupResource(), name)
}

func testView(namespace, name string, mode SubmitMode, expected, observed int) *group.View {
	key := group.Key{Namespace: namespace, Name: name, UID: types.UID(namespace + "/" + name)}
	view := &group.View{Group: &scheduling.PodGroup{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name, UID: key.UID},
		Spec:       scheduling.PodGroupSpec{Members: int32(expected), SubmitMode: string(mode)},
	}}
	controller := true
	for i := 0; i < observed; i++ {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: fmt.Sprintf("pod-%d", i),
				UID: types.UID(fmt.Sprintf("%s/pod-%d", key.UID, i)),
				OwnerReferences: []metav1.OwnerReference{{APIVersion: scheduling.GroupVersion.String(),
					Kind: "PodGroup", Name: name, UID: key.UID, Controller: &controller}}},
			Status: corev1.PodStatus{Phase: corev1.PodPending},
		}
		view.Members = append(view.Members, group.PodView{Pod: pod, Group: key})
	}
	return view
}

func queueFor(views ...*group.View) *WorkQueue {
	w := New()
	reader := make(testViews)
	for _, view := range views {
		reader[view.Group.Namespace+"/"+view.Group.Name] = view
	}
	w.Views = reader
	return w
}

func selectOK(t *testing.T, w *WorkQueue, request SelectRequest) {
	t.Helper()
	if err := w.Select(request); err != nil {
		t.Fatal(err)
	}
}

// 完成所有事件处理后读取队列，避免通过等待超时判断“没有重复入队”。
func drainActive(t *testing.T, w *WorkQueue, expected int) []Unit {
	t.Helper()
	q := w.Active.(*activeQueue)
	q.mu.Lock()
	count := len(q.items)
	q.mu.Unlock()
	if count != expected {
		t.Fatalf("Active 单元数 = %d，期望 %d", count, expected)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	units := make([]Unit, 0, expected)
	for i := 0; i < expected; i++ {
		unit, err := w.Active.Pop(ctx)
		if err != nil {
			t.Fatal(err)
		}
		units = append(units, unit)
	}
	return units
}

func TestSelectRoutesBySourceAndMode(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		mode                   SubmitMode
		podEvent               bool
		wantUnits, wantMembers int
		wantScope              SelectionScope
	}{
		{"atomic/group", AllTogether, false, 1, 3, GroupScope},
		{"atomic/pod", AllTogether, true, 1, 3, GroupScope},
		{"partial/group", AllowPartial, false, 1, 3, GroupScope},
		{"partial/pod", AllowPartial, true, 1, 3, PodScope},
	} {
		t.Run(tc.name, func(t *testing.T) {
			view := testView("work", "job", tc.mode, 3, 3)
			w := queueFor(view)
			w.Policy = func(input SelectionInput, unit Unit) (QueueDecision, error) {
				if input.Scope != tc.wantScope {
					t.Errorf("Scope = %s，期望 %s", input.Scope, tc.wantScope)
				}
				return DefaultPolicy(input, unit)
			}
			request := SelectRequest{View: view}
			if tc.podEvent {
				request = SelectRequest{Pod: view.Members[1].Pod}
			}
			selectOK(t, w, request)
			selectOK(t, w, request) // 重复事件不增加单元。
			for _, unit := range drainActive(t, w, tc.wantUnits) {
				if len(unit.Members()) != tc.wantMembers || !unit.Frozen() {
					t.Fatalf("成员范围或冻结状态错误: %+v", unit.Members())
				}
				if unit.State().Queue != QueueActive || unit.State().Phase != UnitQueued {
					t.Fatalf("首次入队状态错误: %+v", unit.State())
				}
				if err := unit.AddMember(PodRef{Name: "late", UID: "late"}); !errors.Is(err, ErrUnitFrozen) {
					t.Fatalf("冻结后追加成员: %v", err)
				}
				if err := unit.RemoveMember(unit.Members()[0]); !errors.Is(err, ErrUnitFrozen) {
					t.Fatalf("冻结后移除成员: %v", err)
				}
			}
			selectOK(t, w, request) // 出队后事件也不能重新首次入队，消费生命周期尚待接入。
			drainActive(t, w, 0)
		})
	}
}

func TestSelectAtomicWaitsThenReusesAssembly(t *testing.T) {
	full := testView("work", "job", AllTogether, 3, 3)
	partial := *full
	partial.Members = full.Members[:1]
	w := queueFor(&partial)
	selectOK(t, w, SelectRequest{View: &partial})
	drainActive(t, w, 0)
	key := full.Members[0].Group
	id := w.groups[key].pods[full.Members[0].Pod.UID].UnitID
	selectOK(t, w, SelectRequest{View: full})
	units := drainActive(t, w, 1)
	if units[0].Key().ID != id || len(units[0].Members()) != 3 {
		t.Fatal("成员到齐后应复用原组装单元")
	}
}

func TestSelectPartialDoesNotWaitForWholeGroup(t *testing.T) {
	view := testView("work", "job", AllowPartial, 10, 1)
	w := queueFor(view)
	selectOK(t, w, SelectRequest{Pod: view.Members[0].Pod})
	drainActive(t, w, 1)
}

func TestPartialAssemblyBatchesFreeMembersAndKeepsFrozenUnit(t *testing.T) {
	full := testView("work", "batch", AllowPartial, 6, 5)
	first := *full
	first.Members = full.Members[:3]
	w := queueFor(&first)
	// Buffer 正在处理第一个成员，本轮只能组装剩下两个。
	w.Staging = testStaging{key: full.Members[0].Group, uid: full.Members[0].Pod.UID}
	selectOK(t, w, SelectRequest{Pod: first.Members[1].Pod})
	u := drainActive(t, w, 1)[0]
	if len(u.Members()) != 2 {
		t.Fatalf("应批量组装两个空闲成员: %+v", u.Members())
	}
	w.Views = testViews{"work/batch": full}
	selectOK(t, w, SelectRequest{Pod: full.Members[3].Pod})
	next := drainActive(t, w, 1)[0]
	if len(u.Members()) != 2 || next.Key() == u.Key() || len(next.Members()) != 2 {
		t.Fatal("新成员应另成一批，不能改变已经冻结的旧 Unit")
	}
	for _, ref := range append(u.Members(), next.Members()...) {
		if ref.UID == full.Members[0].Pod.UID {
			t.Fatal("重复接收了 Buffer 中的成员")
		}
	}
}

func TestPartialAssemblyReusesUnfrozenUnit(t *testing.T) {
	full := testView("work", "growing", AllowPartial, 10, 4)
	first := *full
	first.Members = full.Members[:1]
	w := queueFor(&first)
	w.Policy = nil
	selectOK(t, w, SelectRequest{View: &first})
	record := w.groups[full.Members[0].Group]
	id := record.pods[full.Members[0].Pod.UID].UnitID
	selectOK(t, w, SelectRequest{View: full})
	if len(record.units) != 1 || len(record.units[id].Members()) != 4 {
		t.Fatal("未冻结的 Partial 应能继续收集同组成员")
	}
	w.Policy = DefaultPolicy
	selectOK(t, w, SelectRequest{View: full})
	if got := drainActive(t, w, 1)[0]; got.Key().ID != id {
		t.Fatal("不应另建单元")
	}
}

func TestSelectReplacesRecreatedPodBeforeFreeze(t *testing.T) {
	full := testView("work", "job", AllTogether, 2, 2)
	old := full.Members[0].Pod.DeepCopy()
	old.UID = "old-member"
	initial := *full
	initial.Members = []group.PodView{{Pod: old, Group: full.Members[0].Group}}
	w := queueFor(&initial)
	selectOK(t, w, SelectRequest{View: &initial})
	drainActive(t, w, 0)
	selectOK(t, w, SelectRequest{View: full})
	units := drainActive(t, w, 1)
	for _, member := range units[0].Members() {
		if member.UID == old.UID {
			t.Fatal("旧 UID 不应保留在本轮成员中")
		}
	}
}

type testStaging struct {
	key group.Key
	uid types.UID
}

func (s testStaging) ContainsPod(key group.Key, uid types.UID) bool {
	return key == s.key && uid == s.uid
}

func TestSelectHonorsStagingLookupBoundary(t *testing.T) {
	// 这里只验证查询契约，不模拟尚未实现的暂存区交接或 binding。
	for _, mode := range []SubmitMode{AllTogether, AllowPartial} {
		t.Run(string(mode), func(t *testing.T) {
			view := testView("work", "job", mode, 2, 2)
			w := queueFor(view)
			w.Staging = testStaging{key: view.Members[0].Group, uid: view.Members[0].Pod.UID}
			selectOK(t, w, SelectRequest{View: view})
			want := 0
			if mode == AllowPartial {
				want = 1
			}
			for _, unit := range drainActive(t, w, want) {
				for _, member := range unit.Members() {
					if member.UID == view.Members[0].Pod.UID {
						t.Fatal("已暂存成员被重复入队")
					}
				}
			}
		})
	}
}

func TestSelectIgnoresUngroupedAndStalePodEvents(t *testing.T) {
	view := testView("work", "job", AllTogether, 1, 1)
	w := queueFor(view)
	ungrouped := view.Members[0].Pod.DeepCopy()
	ungrouped.OwnerReferences = nil
	oldPod := view.Members[0].Pod.DeepCopy()
	oldPod.UID = "old-pod"
	oldGroup := view.Members[0].Pod.DeepCopy()
	oldGroup.OwnerReferences[0].UID = "old-group"
	for _, pod := range []*corev1.Pod{ungrouped, oldPod, oldGroup} {
		selectOK(t, w, SelectRequest{Pod: pod})
	}
	if len(w.groups) != 0 {
		t.Fatal("忽略的事件不应建立组装状态")
	}
	drainActive(t, w, 0)
}

func TestSelectDoesNotScheduleBoundTerminalOrDeletingPods(t *testing.T) {
	for _, kind := range []string{"bound", "succeeded", "failed", "deleting", "group-deleting"} {
		t.Run(kind, func(t *testing.T) {
			view := testView("work", "job", AllowPartial, 1, 1)
			pod := view.Members[0].Pod
			now := metav1.Now()
			switch kind {
			case "bound":
				pod.Spec.NodeName = "node-1"
			case "succeeded":
				pod.Status.Phase = corev1.PodSucceeded
			case "failed":
				pod.Status.Phase = corev1.PodFailed
			case "deleting":
				pod.DeletionTimestamp = &now
			case "group-deleting":
				view.Group.DeletionTimestamp = &now
			}
			w := queueFor(view)
			selectOK(t, w, SelectRequest{Pod: pod})
			drainActive(t, w, 0)
		})
	}
}

func TestSelectRoutesAssembledUnitFailureToFail(t *testing.T) {
	for _, mode := range []SubmitMode{AllTogether, AllowPartial} {
		t.Run(string(mode), func(t *testing.T) {
			view := testView("work", "job", mode, 3, 3)
			w := queueFor(view)
			w.Policy = nil
			selectOK(t, w, SelectRequest{View: view})
			key := view.Members[0].Group
			failed := w.groups[key].pods[view.Members[1].Pod.UID]
			failed.LastFailure = &group.FailureInfo{Reason: "SchedulingFailed", Message: "confirmed by test"}
			w.Policy = DefaultPolicy
			selectOK(t, w, SelectRequest{View: view})
			failure, ok := w.Fail.Get(UnitKey{Group: key, ID: failed.UnitID})
			if !ok || failure.Reason != "SchedulingFailed" {
				t.Fatal("已确认异常未进入 Fail")
			}
			// 组装好之后才出现失败，当前首次入队策略仍把整个候选 Unit 放进 Fail。
			// 后续如何拆出失败成员，不属于本次组装修改。
			if len(failure.Unit.Members()) != 3 || failure.Unit.State().Queue != QueueFail {
				t.Fatal("失败候选 Unit 未正确保留")
			}
			drainActive(t, w, 0)
		})
	}
}

func TestPartialAssemblyKeepsKnownFailuresSeparate(t *testing.T) {
	full := testView("work", "mixed", AllowPartial, 3, 3)
	first := *full
	first.Members = full.Members[:1]
	w := queueFor(full)
	w.Policy = nil
	selectOK(t, w, SelectRequest{View: &first})
	key := full.Members[0].Group
	failed := w.groups[key].pods[full.Members[0].Pod.UID]
	failed.LastFailure = &group.FailureInfo{Reason: ReasonInsufficientResources}
	w.Policy = DefaultPolicy
	selectOK(t, w, SelectRequest{View: full})
	failure, exists := w.Fail.Get(UnitKey{Group: key, ID: failed.UnitID})
	if !exists || len(failure.Unit.Members()) != 1 {
		t.Fatal("已有失败的成员应单独保留在 Fail")
	}
	healthy := drainActive(t, w, 1)[0]
	if len(healthy.Members()) != 2 {
		t.Fatal("正常的两个成员应一起组装，不被失败成员拖入 Fail")
	}
}

func TestSelectDefersInternalInFlightStates(t *testing.T) {
	for _, phase := range []group.PodSchedulingPhase{group.PodReserved, group.PodBinding, group.PodBindingUnknown} {
		t.Run(string(phase), func(t *testing.T) {
			view := testView("work", "job", AllowPartial, 1, 1)
			w := queueFor(view)
			w.Policy = nil
			selectOK(t, w, SelectRequest{View: view})
			w.groups[view.Members[0].Group].pods[view.Members[0].Pod.UID].Phase = phase
			w.Policy = DefaultPolicy
			selectOK(t, w, SelectRequest{Pod: view.Members[0].Pod})
			drainActive(t, w, 0)
		})
	}
}

func TestSelectConcurrentMixedGroupAndPodEvents(t *testing.T) {
	const groupCount, members = 24, 4
	views := make([]*group.View, 0, groupCount)
	for i := 0; i < groupCount; i++ {
		mode := AllTogether
		if i%2 != 0 {
			mode = AllowPartial
		}
		// 各组中的 Pod 名字相同，验证不同组及 namespace 不串成员。
		views = append(views, testView(fmt.Sprintf("ns-%d", i%3), fmt.Sprintf("job-%d", i), mode, members, members))
	}
	w := queueFor(views...)
	var wg sync.WaitGroup
	errs := make(chan error, groupCount*(members+1)*2)
	for _, view := range views {
		requests := []SelectRequest{{View: view}}
		for _, member := range view.Members {
			requests = append(requests, SelectRequest{Pod: member.Pod})
		}
		for _, request := range requests {
			for repeat := 0; repeat < 2; repeat++ {
				wg.Add(1)
				go func(request SelectRequest) {
					defer wg.Done()
					if err := w.Select(request); err != nil {
						errs <- err
					}
				}(request)
			}
		}
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if t.Failed() {
		return
	}
	seen := make(map[types.UID]bool)
	for _, unit := range drainActive(t, w, groupCount) {
		for _, pod := range unit.Members() {
			if seen[pod.UID] {
				t.Fatalf("Pod 重复归入不同单元: %s", pod.UID)
			}
			seen[pod.UID] = true
			record := w.groups[unit.Key().Group]
			if record == nil || record.pods[pod.UID] == nil || record.pods[pod.UID].UnitID != unit.Key().ID {
				t.Fatal("队列成员与组/Pod 归属记录不一致")
			}
		}
	}
	if len(seen) != groupCount*members {
		t.Fatalf("覆盖 Pod 数 = %d", len(seen))
	}
}

func TestSelectValidatesRequest(t *testing.T) {
	view := testView("work", "job", AllTogether, 1, 1)
	w := queueFor(view)
	for _, request := range []SelectRequest{{}, {View: view, Pod: view.Members[0].Pod}} {
		if err := w.Select(request); err == nil {
			t.Fatal("应拒绝无来源或双来源请求")
		}
	}
}

func TestSelectDispatchesToEveryQueue(t *testing.T) {
	// 注入决策只测试分发能力，不代表默认策略已实现 Retry/FastRetry 准入。
	for _, target := range []QueueName{QueueActive, QueueFail, QueueRetry, QueueFastRetry} {
		t.Run(string(target), func(t *testing.T) {
			view := testView("work", "job", AllTogether, 1, 1)
			w := queueFor(view)
			w.Policy = func(SelectionInput, Unit) (QueueDecision, error) {
				return QueueDecision{To: target, Reason: "TestDecision", WaitingFor: "test-condition"}, nil
			}
			selectOK(t, w, SelectRequest{View: view})
			record := w.groups[view.Members[0].Group]
			id := record.pods[view.Members[0].Pod.UID].UnitID
			unit := record.units[id]
			if unit.State().Queue != target || unit.State().Phase != UnitQueued {
				t.Fatalf("分发后的状态不一致: %+v", unit.State())
			}
			switch target {
			case QueueActive:
				drainActive(t, w, 1)
			case QueueFail:
				failure, ok := w.Fail.Get(unit.Key())
				if !ok || failure.Unit != unit || failure.Reason != "TestDecision" {
					t.Fatal("Fail 未保存单元及原因")
				}
			case QueueRetry:
				retry, ok := w.Retry.Get(unit.Key())
				if !ok || retry.Unit != unit || retry.WaitingFor != "test-condition" {
					t.Fatal("Retry 未保存单元及等待条件")
				}
			case QueueFastRetry:
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				got, err := w.FastRetry.Pop(ctx)
				if err != nil || got != unit {
					t.Fatalf("FastRetry 未收到单元: %v", err)
				}
			}
			if target != QueueActive {
				drainActive(t, w, 0)
			}
			selectOK(t, w, SelectRequest{View: view})
			drainActive(t, w, 0)
		})
	}
}
