# 通信放置插件：这轮代码怎样工作

更新日期：2026-10-27。本文解释当前本地实现。

**本轮小步更新：resolveLinks 已按 Priority 从高到低、同值按名称排序。Planner 已沿用通信搜索的排序选定第一套完整方案；集中评分及多语义之间的最终取舍仍未实现。**

这轮实现的是：**把本轮成员组合成少量可行放置方案，并带出通信评价。** 当前只处理通信两端都在本轮 Unit 的情况。

集中插件、多语义汇总和 Bind 仍未实现。Assume/Forget 已接入执行线；当前在 Bind 占位处撤销暂占并报告 Blocked，不会实际绑定 Pod。

## 1. 先只看 Place 主函数

入口是 [placement.go](../src/scheduler/plugins/communication/placement.go)。它只安排三步：

```text
resolveLinks：把稳定成员 ID 换成实际 Pod，按 Priority 从高到低排序
    ↓
逐条 placeLink：在已有方案上增加关系，累计检查资源，尝试不同靠近等级
    ↓
逐个 completeMember：补上没有通信关系的成员
    ↓
返回 []CandidatePlan
```

Place 不操作队列，不修改 Buffer，不发绑定请求。后面的 Planner 继续处理这些方案。

## 2. 输入已经怎样传过来

```text
PodGroup.spec
    ↓ Select 组装时复制
Unit.Definition：组 Generation＋PodGroupSpec
    ↓ Buffer 准备 Execution 时复制
Execution.Definition
    ↓ Score 读取本轮声明，复制 Placement
GroupScoreInput.Placement
    ↓
Place
```

[Definition](../src/group/definition.go) 直接保存 API 的 Spec，不另抄一套配置字段。Unit 冻结后，声明也不能替换。

这里携带完整 Group 的声明，不把通信关系截成“只剩本轮成员的几条”。当前算法暂时只支持两端都在本轮的关系；其他来源还没有实现时返回明确的待实现错误。

## 3. resolveLinks：声明变成实际成员

[links.go](../src/scheduler/plugins/communication/links.go) 先建立：

```text
member-id → 本轮 Pod
```

随后每条关系得到 Rule、From Pod、To Pod，并按 Priority 从高到低排序，同值按名称排序。后面的搜索先扩展重要关系，比较方案时也先比较这些关系的降级程度。排序的是新建的 link 列表，不修改输入声明。

成员的两个 NodeList 从 `input.PodScores[PodRef]` 取得，已经由前面的 Pod Score 按分数排序。

标签重复、关系声明完整校验，以及端点在其他 Unit 或已绑定成员中的解析，仍待逐项补充。

## 4. trial：一套正在构造的方案

[trial.go](../src/scheduler/plugins/communication/trial.go) 的 trial 只有三类数据：

- plan：目前各成员选了哪个 Node，以及累积评分和关系结果。
- nodes：这套方案自己的 NodeInfo 副本，包含已经试放的成员占用。
- degradation：按关系处理顺序记录各自降级几步，方便优先保住重要关系。

每次尝试另一个选择，先 clone 当前 trial。新选择失败，丢掉副本即可，原来的方案还在。这样主流程不用混入一堆撤销资源的方法。

`trial.place` 真正增加一个成员：

1. 已有位置的共享成员，只核对节点一致，不重复计资源和 Pod 分数。
2. 调用上游检查，看累计资源和 HostPort 是否仍允许放入。
3. 成功则把 PodInfo 加到本方案的 NodeInfo，记录位置和基础分。

[FitsForPlacement](../src/scheduler/upstream/placement.go) 复用上游 NodeResourcesFit 和 NodePorts 的检查。它只在原有 Filter 候选上重查新增占用，不替代整个 Pod Filter。

整个过程没有修改 Pod.spec.nodeName，也没有实际预留资源。

## 5. placeLink：把一条链路加入当前组方案

代码在 [search.go](../src/scheduler/plugins/communication/search.go)。

对每套已有方案，拿两端候选列表，依次尝试 Node、Rack、Zone、Cluster：

```text
从两端高分候选开始取一个 Node 对
    ↓
判断这对节点实际处于哪个靠近等级
    ↓
复制当前方案，依次尝试放入两端
    ↓
资源足够：保留扩展后的方案及通信评价
资源不足：丢掉这个尝试，继续下一个 Node 对
```

两端恰好在同一个节点，就是同节点候选。两端在不同节点，则根据标签判断机架和可用区。

如果某端在前面的链路中已经放好，它的候选列表只剩那个位置。这样 A—B 和 A—C 中的 A 不会被安排到两个地方。

我们会保留更远的备选，给后面的资源整合评价留下选择。期望等级与实际等级逐条存入 CandidatePlan.Communication；Notes 另提供直白的说明。后面的汇总无需解析说明字符串来找回权重或优先级。

## 6. NodeInfo 为什么必须累计

假设每个节点还剩 4 CPU，A、B、C 各要 2 CPU：

```text
先放高优先级 A—B：A、B 同 node1，占满 4 CPU
再放低优先级 A—C：沿用 A 的位置
                  C 加入 node1 会超量
                  尝试把 C 放到同机架 node2
```

A 不会再扣一次资源；C 也不能因为单独通过过 node1 的 Filter，就忽略此刻 node1 已被 A、B 占满。

这正是组联合放置与“各 Pod 各取最高分”的区别。

## 7. 候选怎样控制数量

`Plugin.MaxPlans` 默认 4。每套已有方案、每个靠近等级最多收集 4 个成功扩展，再由 [keepTrials](../src/scheduler/plugins/communication/candidates.go) 保留总共至多 4 套。

先按 Priority 排好的关系顺序逐项比较降级程度，再比较通信分和 Pod 基础分。之后优先保留不同形状，例如 `[2]` 表示两个成员集中一台，`[1 1]` 表示分散两台；还有空位再补其他候选。相同放置不重复保留。

从搜索树看，trial 是一个部分方案，placeLink 扩展下一条关系，keepTrials 是每层扩展后的剪枝。Priority 引导搜索和比较顺序，不把降级当成不可行；资源或端口不满足才淘汰这次试放。形状只是按节点上的成员数量区分布局，不能保证保留下所有资源整合更好的方案。后续评分只能比较保留的完整候选，不能找回已剪掉的分支。

这是第一版的有限候选搜索，会舍弃一些中间组合。因此没有找到方案时，错误描述的是“有限候选内没有找到”，不能据此证明集群不存在任何可行解。

保留数量有限，不代表最多只检查四个 Node 对。失败尝试仍可能扫描较多候选；当前响应 ctx 取消，没有加入独立时间预算或后台搜索。

## 8. 当前通信分怎样算

本轮先采用一套方便解释的原始分：

```text
靠近分：Node=3，Rack=2，Zone=1，Cluster=0
通信分：所有关系的 weight × 实际靠近分，相加
Pod 分：每个成员所选 Node 的基础分，相加，每个成员只计一次
```

Weight 用于通信分，Priority 用于关系处理及降级比较的顺序。它们不是同一个参数：高 Priority 关系先受保护，Weight 在降级记录一致后影响通信分比较。仍会保留不同布局，Priority 不意味着所有候选都必须优先满足同一条关系；最终如何权衡其他语义尚待实现。

当前最终选择沿用搜索排序，不重新加总这些分数。集中分、各项归一化和权重尚未确定，不把几个原始分直接相加后用于绑定。

### 8.1 selectBest：首版怎样选定计划

[selection.go](../src/scheduler/planner/selection.go) 接收同一轮输入和排好序的候选，取第一套，核对它是否恰好包含本轮所有成员、每个成员的节点是否在本轮快照中，再复制位置表交给 Executor。累计资源和端口可行性由 Place 试放负责，这里不重新运行一遍搜索。

这表示“在当前保留的候选中，按通信搜索已有排序选第一套”，不承诺全局最优。其他 Placement 插件也须按自己的偏好由好到差返回候选。

ConcentrationPlugin.Score 接口保留。若已产生额外集中分，但通信与集中之间的比较规则尚未实现，selectBest 返回明确的待实现错误，不默默忽略这项评分。

## 9. 拓扑标签怎样配置

`communication.New()` 默认：

- RackLabel：`scheduling.batchscheduler.io/rack`，这是本项目约定的可配置标签。
- ZoneLabel：`topology.kubernetes.io/zone`。
- 同节点直接比较节点名。

使用其他机架标签时，在启动前设置 Plugin.RackLabel。缺少标签时不假定它们处于同一机架或可用区，继续按能确认的更大范围评价。

拓扑距离仍是近似，不是实测带宽或延迟。

## 10. 自定义插件放在哪里

[scheduler/plugins](../src/scheduler/plugins/doc.go) 是实现目录，[planner/plugins.go](../src/scheduler/planner/plugins.go) 是接口定义。

默认 Runner 接入 communication.New()。用户可以另写实现 LinkPlacementPlugin 的类型，在启动前通过 Executor.SetPlanner 替换；不需要让 Buffer 知道具体算法。

当前默认路径是：

```text
Executor → steps.PlanGroup → Planner.Plan → communication.Plugin.Place
                                            ↓
                               候选方案交回 Planner
                                            ↓
                               可选集中评分 → selectBest 选定
                                            ↓
                               Report Planned → Assume → Report Assumed
                                            ↓
                               Bind 待实现 → Forget → Blocked
```

## 11. 本轮检查范围

- 通信插件测试覆盖不同端点基础分、不同布局、累计资源、共享成员、HostPort 冲突、缺少机架标签、未关联成员补齐及无通信声明；另检查 Priority 不受声明顺序影响、同值按名称，以及优先保护一条关系时仍保留其他放法。
- 声明快照检查：入队后改原声明或查询返回值，不影响冻结 Unit；Buffer、Score 继续传递独立副本。
- 选定测试检查沿用通信排序、返回独立位置表，以及不完整方案和额外评分未实现的情况。
- 执行线测试检查：Planned → Assume → Assumed；Bind 占位未开始时先 Forget 再 Blocked，不留下暂占。另覆盖选定后资源变化导致 Assume 失败，经 Report 回 Fail，再接 Transition → Retry。

排队期间修改组声明会被 Accept 拒绝；后续自动重组旧队列任务尚未完善。本轮没有做真实集群、网络性能或大规模搜索压测。

## 12. 怎样继续 review 和推导

先只走一个具体场景：A、B 各需 2 CPU，试着一起放到剩余 4 CPU 的节点上。

1. 看 Place 怎样取得两个 Pod 及其 NodeList，再看 placeLink 的四层遍历。
2. 看 tryLink 怎样复制旧方案、先放 A 再放 B，以及 trial.place 怎样累计占用。
3. 加入共享 A 的第二条链路 A—C，再对照 Priority 排序、降级数组与两种冲突妥协方案。
4. 最后才看候选保留：哪些组合被丢掉、不同形状如何留下、候选数量限制会怎样影响结果。

我们需要共同确认的算法规则放在文档里，主函数的注释只讲代码当前怎样做：

- Priority 怎样决定链路处理顺序，Weight 怎样参与评价，两者是否符合预期。
- 同一端点跨多条关系时，位置固定与累计占用怎样推导。
- 有近距离方案时仍保留远距离备选，这与最终评分怎样衔接。
- 每轮最多保留 K 套，可能漏掉哪些后续才变得可行的组合。
- 拓扑标签如何表达真实的域，缺少标签或标签层级不一致时怎样解释结果。
- 节点数、成员数和链路数增加时，节点对枚举与资源复制各有多少工作量。

这份实现是待共同核对的原型，不因测试曾通过就当成算法推导已经完成。下一次逐个方法过，不一次接受全部实现。
