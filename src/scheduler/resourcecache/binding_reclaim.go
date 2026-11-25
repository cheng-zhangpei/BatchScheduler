package resourcecache

import (
	"fmt"
	"sort"
	"time"

	"batchscheduler/src/workqueue"
)

// BindingReclamation 区分真实绑定和按核查策略释放的成员。
// Ready=false 表示还有成员需要继续核查，本次没有释放任何暂占。
type BindingReclamation struct {
	Bound      []workqueue.PodRef
	Released   []workqueue.PodRef
	Ready      bool
	Mismatched bool // 有成员实际绑定到了原计划以外的节点。
}

// ReclaimBindingChecks 在一把资源锁内复核整轮、Forget 未绑定成员、保存历史。
// 原截止时间到了就回收，不等额外 GET；已经确认的真实绑定仍转入 Bound。
func (c *Cache) ReclaimBindingChecks(keys []BindingCheckKey, now time.Time) (BindingReclamation, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(keys) == 0 {
		return BindingReclamation{}, nil
	}
	first := keys[0]
	result := BindingReclamation{Ready: true}
	var forget []workqueue.PodRef
	for _, key := range keys {
		check, exists := c.bindingChecks[key]
		if !exists {
			return BindingReclamation{}, nil
		}
		if key.Unit != first.Unit || key.ActionID != first.ActionID {
			return BindingReclamation{}, fmt.Errorf("绑定回收只能处理同一次执行")
		}
		pod := c.pods[key.Pod.UID]
		assumption, assumed := c.assumed[key.Pod.UID]
		if !assumed && pod != nil && pod.Spec.NodeName != "" && check.State != BindingCheckGone {
			result.Bound = append(result.Bound, key.Pod)
			result.Mismatched = result.Mismatched || pod.Spec.NodeName != check.NodeName
			continue
		}
		// 已释放的记录可能因 Report 失败留在这里；再次处理不重复扣账。
		if check.State == BindingCheckReleased {
			result.Released = append(result.Released, key.Pod)
			continue
		}
		if check.State != BindingCheckGone && !check.ReadyToRelease(now) {
			return BindingReclamation{}, nil
		}
		if assumed {
			if assumption.Unit != key.Unit || assumption.ActionID != key.ActionID {
				return BindingReclamation{}, fmt.Errorf("待回收 Pod %s 已属于其他执行", key.Pod.Name)
			}
			forget = append(forget, key.Pod)
		}
		result.Released = append(result.Released, key.Pod)
	}
	if err := c.forget(first.Unit, first.ActionID, forget); err != nil {
		return BindingReclamation{}, err
	}
	for _, ref := range result.Released {
		key := BindingCheckKey{Unit: first.Unit, ActionID: first.ActionID, Pod: ref}
		check := c.bindingChecks[key]
		if check.State == BindingCheckGone || check.State == BindingCheckReleased {
			continue
		}
		check.State, check.ReleasedAt = BindingCheckReleased, now
		c.bindingChecks[key] = check
		c.releasedChecks.remember(check)
	}
	// 重试 Report 时保留相同成员顺序，Buffer 可以识别重复反馈。
	sort.Slice(result.Released, func(i, j int) bool {
		if result.Released[i].Name == result.Released[j].Name {
			return result.Released[i].UID < result.Released[j].UID
		}
		return result.Released[i].Name < result.Released[j].Name
	})
	return result, nil
}
