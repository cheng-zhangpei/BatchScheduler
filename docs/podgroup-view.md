# PodGroup 与 Pod 视图

本步建立声明、成员及关系，不定义工作队列或调度状态机。

## 两类对象的职责

- PodGroup CR：`scheduling.batchscheduler.io/v1alpha1`，namespace 级别。`spec.members` 表示期望成员总数，`spec.submitMode` 表示提交模式，可选的 `spec.placement` 声明通信关系及资源整合偏好。没有 Pod 模板、minMember、重试策略和运行状态字段。
- Pod：保存成员完整的 spec 和运行事实。未来由 operator 根据组的声明创建、维护，调度器不创建成员。

CRD 在 `config/crd/scheduling.batchscheduler.io_podgroups.yaml`，Go 数据定义在 `src/apis/scheduling/v1alpha1/types.go`。目前通过 dynamic client 读取 CR，再转成 Go 结构；尚未生成 typed clientset，也不需要把这个 Go 结构注册成 runtime.Object。operator 的完整 API 与代码生成在后续阶段设计。

## 唯一的成员关联规则

operator 创建 Pod 时，应设置指向 PodGroup 的 controller ownerReference：

- `apiVersion` 必须为 `scheduling.batchscheduler.io/v1alpha1`。
- `kind` 必须为 `PodGroup`。
- `name` 和 `uid` 对应真实组对象，`controller` 为 true。
- Pod 与组处于同一 namespace。

当前约定 Pod 由 PodGroup 直接拥有，不经过 Job 等中间对象。若后续采用中间控制器，再明确调整关联协议。

调度器以 namespace 和组 UID 建立成员索引，查询时同时核对组名。组删除后同名重建会得到新 UID，旧 Pod 不会自动归入新组。不依据同名、相似 spec 或标签猜测成员关系；没有有效组引用的 Pod 仍保留在普通 Pod 缓存中，供以后观察资源占用。

operator 负责写入这份关系，调度器负责读取并识别关系。无需让两者各自维护一份独立成员清单。

## 通信关系怎样引用成员

2026-10-26 增加配置结构，示例见 [podgroup-communication.yaml](../config/examples/podgroup-communication.yaml)，Go 定义见 [placement.go](../src/apis/scheduling/v1alpha1/placement.go)。

成员 Pod 使用 `scheduling.batchscheduler.io/member-id` 标签表达稳定身份，例如 `worker-0`。Operator 重建 Pod 时保留该值。通信关系的 `from`、`to` 引用这个值；实际执行仍使用 Pod 名称与 UID 区分本次对象。

标签只用于组内通信端点，不改变 ownerReference 的组归属规则。相同 member-id 可以出现在不同 Group 内。

| 字段 | 含义 |
| --- | --- |
| placement.communication[].name | 关系名称，在本组中唯一 |
| from、to | 同组两端的稳定成员标签值 |
| preferredLevel | 希望同 Node、Rack、Zone 或 Cluster；冲突时允许扩大范围 |
| weight | 正数，表达通信评分的重要程度，可以相同 |
| priority | 数值越大越优先保住；较小的先降级，与 Pod 优先级无关 |
| placement.resourceConsolidation | 是否启用额外的组级资源整合评价，默认关闭 |

声明已沿 Unit → Execution → GroupScoreInput 透传，本轮通信端点可按稳定标签解析。Unit 保存冻结的组声明副本；排队后声明变化时，Accept 会拒绝旧轮次，自动重组旧队列任务还待完善。重复标签和完整关系校验、跨 Unit 端点解析继续后置。

当前通信插件的 RackLabel 默认 `scheduling.batchscheduler.io/rack`，可由启动配置替换；ZoneLabel 默认 `topology.kubernetes.io/zone`。没有标签时不假定同域。源码说明见[通信放置插件](communication-placement.md)。

## 本地视图如何建立

1. Pod informer 更新对象缓存时，Indexer 自动维护成员索引，新增、更新、删除都会反映到后续查询。
2. PodGroup informer 缓存组声明。
3. `observed.GroupViews.Get(namespace, name)` 获取组声明，按当前组 UID 查询 Pod 索引，得到 `group.View`。

`View.Group` 是解析后的组 CR；`View.Members` 是成员 PodView 列表，每个 PodView 包含完整 Pod 和组身份。返回成员按 Pod 名字排序，对象与缓存脱离，调用者修改视图不会修改 informer 中的共享对象。

视图保留已经绑定、运行中、删除中和终态成员，不只挑 Pending Pod，也不根据 schedulerName 在此处过滤。`spec.members` 是期望数，`len(view.Members)` 是当前观察数量，两者可以暂时不同。什么成员可计入门槛、哪些组可入队，留到下一步讨论，不在这层隐式决定。

这是查询时组装的观察视图，没有第二套手工增减的成员映射。Pod 和 CR 来自不同缓存，查询不构成跨对象原子快照；后续真正调度时仍需处理对象变化。

## 本次边界

程序现在需要 Pod、Node、PodGroup 三种资源完成初始同步。CRD 需提前安装，并提供对应 list/watch 权限。尚未实现 operator、CR 创建流程、事件入队、调度或绑定。本次不部署 CRD、不连接集群、不运行测试。
