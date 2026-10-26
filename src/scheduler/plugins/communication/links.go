package communication

import (
	"fmt"

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
// 当前只查传入的本轮 pods；返回顺序与 rules 相同，这个函数没有按 Priority 排序。
func resolveLinks(pods []*corev1.Pod, rules []scheduling.CommunicationLink) ([]link, error) {
	members := make(map[string]*corev1.Pod, len(pods))
	// 一次建好成员表，后面每条规则只需要各查一次两端。
	for _, pod := range pods {
		members[pod.Labels[scheduling.MemberIDLabel]] = pod
	}
	links := make([]link, 0, len(rules))
	// 规则顺序逐条保留；同一份 Rule 连同解析出的两个 Pod 一起交给 placeLink。
	for _, rule := range rules {
		from, to := members[rule.From], members[rule.To]
		if from == nil || to == nil {
			return nil, fmt.Errorf("%w: 链路 %s 的端点不全在本轮，后续再接其他成员来源", staging.ErrHandlerUnavailable, rule.Name)
		}
		links = append(links, link{Rule: rule, From: from, To: to})
	}
	return links, nil
}
