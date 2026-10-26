package v1alpha1

// MemberIDLabel 标识组内稳定成员，例如 worker-0。
// Operator 重建 Pod 时保留这个标签值；Pod 是否属于该组仍由 ownerReference 决定。
const MemberIDLabel = GroupName + "/member-id"

// TopologyLevel 表达用户希望的靠近程度。实际节点标签由后续拓扑解析代码解释。
type TopologyLevel string

const (
	TopologyNode    TopologyLevel = "Node"
	TopologyRack    TopologyLevel = "Rack"
	TopologyZone    TopologyLevel = "Zone"
	TopologyCluster TopologyLevel = "Cluster"
)

// PlacementSpec 描述整个 Group 的放置偏好，Partial Unit 也需要保留完整声明。
type PlacementSpec struct {
	// Communication 显式列出成员间的通信关系，不要求所有成员两两都有关系。
	Communication []CommunicationLink `json:"communication,omitempty"`
	// ResourceConsolidation 允许额外评价资源碎片；不关闭原有的 Pod 资源检查或基础评分。
	ResourceConsolidation bool `json:"resourceConsolidation,omitempty"`
}

// CommunicationLink 的两端引用同组 Pod 的 MemberIDLabel 值，不引用 Pod 名称或 UID。
// 例如 From=worker-0，后续规划器需要在同组成员中找到标签值为 worker-0 的当前 Pod。
// TODO：接入规划时检查成员标签是否缺失或重复、链路是否自环，以及 Priority 是否重复。
type CommunicationLink struct {
	Name string `json:"name"` // 关系名称，用来解释评分和降级结果，在组内唯一。
	From string `json:"from"`
	To   string `json:"to"`
	// PreferredLevel 是期望等级；放不下时允许按拓扑层级逐步扩大范围。
	PreferredLevel TopologyLevel `json:"preferredLevel"`
	// Weight 越大，满足这条关系对通信评分的贡献越重要；必须为正，可以相同。
	Weight int32 `json:"weight"`
	// Priority 越大，冲突时越优先保住这条关系；低值先降级，不是 Pod 优先级。
	Priority int32 `json:"priority"`
}

// DeepCopy 复制可变的链路列表，避免后续步骤改动本轮保存的声明。
func (p *PlacementSpec) DeepCopy() *PlacementSpec {
	if p == nil {
		return nil
	}
	copy := *p
	if p.Communication != nil {
		copy.Communication = append([]CommunicationLink{}, p.Communication...)
	}
	return &copy
}
