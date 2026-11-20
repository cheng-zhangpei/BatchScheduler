package resourcecache

import (
	"errors"
	"fmt"

	"batchscheduler/src/scheduler/upstream"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// 执行器用这些分类决定等待哪些变化，不解析错误文字。
var (
	ErrAssumeNoFit       = errors.New("当前资源或端口无法承接选定计划")
	ErrAssumeNodeMissing = errors.New("暂占目标节点已不存在")
)

// AssumeResult 区分实际接纳的成员和本次放不下的成员。
// Err 只说明对应成员为什么没接纳，不表示其他成功成员也要撤销。
type AssumeResult struct {
	Assumed  []Assumption
	Rejected []AssumeRejection
}

type AssumeRejection struct {
	Assumption Assumption
	Err        error
}

// Assume 一次登记同一轮执行的整批暂占。每个 Pod 副本的 NodeName 已由调用方填好。
// 先在涉及节点的副本上逐个试放，再统一写回；任一成员失败，三份共享账都不改变。
// 这里复核累计资源和 HostPort，不提交绑定，也不修改 Buffer。
func (c *Cache) Assume(batch []Assumption) error {
	_, err := c.assume(batch, false)
	return err
}

// AssumePartial 按输入顺序尝试成员：资源不足、端口冲突或节点消失时跳过该成员，
// 继续尝试后面的成员。成功部分统一提交；身份冲突等错误仍拒绝本次新增占用。
// 全部放不下时返回空 Assumed，由执行器按整轮失败处理。
func (c *Cache) AssumePartial(batch []Assumption) (AssumeResult, error) {
	return c.assume(batch, true)
}

func (c *Cache) assume(batch []Assumption, partial bool) (AssumeResult, error) {
	prepared, err := prepareAssumptions(batch)
	if err != nil {
		return AssumeResult{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	// 第一段：在最新资源账的副本上累计试放。Atomic 遇到失败整批不写，Partial 跳过放不下的成员。
	nodes := make(map[string]*framework.NodeInfo)
	pending := make([]Assumption, 0, len(prepared))
	var result AssumeResult
	for _, record := range prepared {
		recorded, err := c.alreadyAssumed(record)
		if err != nil {
			return AssumeResult{}, err
		}
		if recorded {
			result.Assumed = append(result.Assumed, record)
			continue
		}
		if err := c.tryAssumption(record, nodes); err != nil {
			if partial && (errors.Is(err, ErrAssumeNoFit) || errors.Is(err, ErrAssumeNodeMissing)) {
				result.Rejected = append(result.Rejected, AssumeRejection{Assumption: record, Err: err})
				continue
			}
			return AssumeResult{}, err
		}
		pending = append(pending, record)
		result.Assumed = append(result.Assumed, record)
	}

	// 第二段：把实际接纳部分一起写入节点总账、Pod 索引和暂占明细。
	// 读取快照和 informer 更新使用同一把锁，只会看见提交前或提交后的完整结果。
	if len(pending) > 0 {
		for name, node := range nodes {
			c.nodes[name] = node
		}
	}
	for _, record := range pending {
		c.pods[record.Pod.UID] = record.Pod
		c.assumed[record.Pod.UID] = record
	}
	return result, nil
}

// tryAssumption 只改本批次的节点副本。成功占用会参与后一个成员的检查。
func (c *Cache) tryAssumption(record Assumption, nodes map[string]*framework.NodeInfo) error {
	pod := record.Pod
	if nodes[pod.Spec.NodeName] == nil {
		current := c.nodes[pod.Spec.NodeName]
		if current == nil || current.Node() == nil {
			return fmt.Errorf("%w: %s", ErrAssumeNodeMissing, pod.Spec.NodeName)
		}
		nodes[pod.Spec.NodeName] = current.SnapshotConcrete()
	}
	node := nodes[pod.Spec.NodeName]
	if !upstream.FitsForPlacement(pod, node) {
		return fmt.Errorf("%w: Pod %s/%s → %s", ErrAssumeNoFit, pod.Namespace, pod.Name, pod.Spec.NodeName)
	}
	info, err := framework.NewPodInfo(pod)
	if err != nil {
		return fmt.Errorf("Assume 读取 Pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	node.AddPodInfo(info)
	return nil
}

// prepareAssumptions 确认这是一轮执行的有效成员集合，再复制 Pod 作为记账依据。
// 拒绝重复 UID，避免同一个 Pod 在一批里被计算两次。
func prepareAssumptions(batch []Assumption) ([]Assumption, error) {
	if len(batch) == 0 {
		return nil, fmt.Errorf("Assume 不能登记空批次")
	}
	owner := batch[0]
	key := owner.Unit.Group
	if owner.Unit.ID == "" || owner.ActionID == "" || key.Namespace == "" || key.Name == "" || key.UID == "" {
		return nil, fmt.Errorf("Assume 缺少完整的组、Unit 或执行编号")
	}
	prepared := make([]Assumption, 0, len(batch))
	seen := make(map[types.UID]bool)
	for _, record := range batch {
		if record.Unit != owner.Unit || record.ActionID != owner.ActionID {
			return nil, fmt.Errorf("Assume 一批只能包含同一 Unit 的同一次执行")
		}
		pod := record.Pod
		if pod == nil || pod.Name == "" || pod.UID == "" || pod.Namespace != key.Namespace || pod.Spec.NodeName == "" {
			return nil, fmt.Errorf("Assume 成员缺少名称、UID、目标节点，或 namespace 与组不一致")
		}
		if seen[pod.UID] {
			return nil, fmt.Errorf("Assume 批次重复包含 Pod %s/%s", pod.Namespace, pod.Name)
		}
		seen[pod.UID] = true
		record.Pod = pod.DeepCopy()
		prepared = append(prepared, record)
	}
	return prepared, nil
}

// alreadyAssumed 让相同执行、相同 Pod 计划的重复调用沿用原占用。
// 其他执行不能覆盖它；同一执行也不能偷偷改变节点或资源需求。
// 调用者已经持有写锁。
func (c *Cache) alreadyAssumed(record Assumption) (bool, error) {
	pod := record.Pod
	if c.pods[pod.UID] == nil {
		return false, nil
	}
	previous, assumed := c.assumed[pod.UID]
	if assumed && previous.Unit == record.Unit && previous.ActionID == record.ActionID &&
		previous.Pod.Name == pod.Name && previous.Pod.Namespace == pod.Namespace &&
		apiequality.Semantic.DeepEqual(previous.Pod.Spec, pod.Spec) {
		return true, nil
	}
	return false, fmt.Errorf("Pod %s/%s 已有其他暂占、不同计划或真实绑定，不能再次 Assume", pod.Namespace, pod.Name)
}
