package communication

import (
	"fmt"
	"sort"

	scheduling "batchscheduler/src/apis/scheduling/v1alpha1"
	"batchscheduler/src/staging"
	corev1 "k8s.io/api/core/v1"
)

// link 把一条用户声明和两个实际 Pod 放在一起。
// Rule 保留权重、优先级和期望等级；From/To 用来查后面的 PodScores。
type link struct {
	Rule scheduling.CommunicationLink
	From *corev1.Pod
	To   *corev1.Pod
}

// resolveLinks 先建立 member-id → Pod 的表，再用每条规则的 From/To 查出两个 Pod。
// 例如规则中的 from=worker-0，查的是 Pod 的稳定标签，不是它当前的 metadata.name。
// 当前只查传入的本轮 pods；解析后按 Priority 从高到低排序，同值按关系名称排序。
// 搜索会先放重要关系；后面比较方案时，也会先比较这些关系降级了几步。
func resolveLinks(pods []*corev1.Pod, rules []scheduling.CommunicationLink) ([]link, error) {
	members := make(map[string]*corev1.Pod, len(pods))
	// 一次建好成员表，后面每条规则只需要各查一次两端。
	for _, pod := range pods {
		members[pod.Labels[scheduling.MemberIDLabel]] = pod
	}
	links := make([]link, 0, len(rules))
	// 将声明和实际端点放在一起；后面只排序这份新列表，不修改用户的声明。
	for _, rule := range rules {
		from, to := members[rule.From], members[rule.To]
		if from == nil || to == nil {
			return nil, fmt.Errorf("%w: 链路 %s 的端点不全在本轮，后续再接其他成员来源", staging.ErrHandlerUnavailable, rule.Name)
		}
		links = append(links, link{Rule: rule, From: from, To: to})
	}
	// 对link的优先级排序了一下
	sort.SliceStable(links, func(i, j int) bool {
		if links[i].Rule.Priority != links[j].Rule.Priority {
			return links[i].Rule.Priority > links[j].Rule.Priority
		}
		return links[i].Rule.Name < links[j].Rule.Name
	})
	return links, nil
}
