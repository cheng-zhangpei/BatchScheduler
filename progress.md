# BatchScheduler 架构与进度

更新日期：2026-10-20。以当前代码和本次讨论为准，不使用百分比估算完成度。

**整体调度流程尚未打通。Center 的 Accept / Observe / Evaluate / Apply / Report / Release 已实现，模拟执行器验证了正常释放和失败恢复回送闭环。main 已接观察、评估、Apply 及新成员 Select；真实执行器、队列侧交接适配器、Active 消费主循环、资源预留和绑定仍未完成。**

### 2026-10-20 收工位置 / 下次继续

- 已补 Apply，并实现最小 Report / Release：重查决策、登记动作身份、锁外分发、反馈推进、交接确认后清理。
- 模拟闭环已通过；下次先复盘具体环节的权衡，再逐个补真实执行器和队列交接适配器，不一口气展开。
- 模拟执行器覆盖 Schedule → Assume → Bind → 释放，以及失败 → Recover → 回送适配器；不等于真实集群调度完成。
- 尚未完成 Active.Pop → Accept 的消费交接、旧 Unit 归属解除、队列侧 Transition 触发及错误回调自动重试。
- compaction、FastRetry 严格准入、Agent 参与等继续后置。保持每次一小块、先理解再推进的节奏。
- 最近验证：`go test ./src/... -count=1` 全部通过；没有运行真实集群测试、竞态检测或性能压测。

## 1. 当前架构边界

| 部分 | 当前职责 | 边界与约定 |
| --- | --- | --- |
| informer / lister | 观察 Pod、Node、PodGroup，维护本地对象缓存 | 不执行 Assume / Forget，不承担调度资源记账 |
| Group Store / View | 通过 ownerReference 和 UID 关联组及成员，形成查询视图 | 复用 informer 索引；不是第二份对象缓存，也不是跨资源原子快照 |
| Unit | 表示同一组内、本轮参与处理的 Pod 集合 | 保留接口和默认实现；组装中可增减成员，冻结后成员固定 |
| Select | 统一接收 Pod 或组视图，补齐上下文、组装单元并首次入队 | 无组 Pod 忽略；Atomic 的 Pod 事件提升到组级，Partial 的 Pod 事件只处理当前成员 |
| Policy | 判断一个候选 Unit 暂不入队或进入哪个队列 | 不计算节点放置，不执行恢复或 API 操作 |
| 四个队列 | 保存 Active、Fail、Retry、FastRetry 的单元和附属信息 | 存取机制已实现；两条基础迁移已实现；自动唤醒和消费策略未实现 |
| Transition | 已入队单元的队列迁移入口 | 已实现 Fail → Retry 和显式提示驱动的 Retry → Active，FastRetry 后置；首次入队不走这里 |
| 缓冲区 / 感知机 | 承接调度与绑定的中间状态，汇总实际结果，沟通 workqueue | Center 接口已实现，模拟闭环通过；实际执行器、队列交接与异常自动重试待接入 |
| 调度周期 / binding cycle / 调度缓存 | 规划负责节点决策、临时占用、绑定及确认 | 均未实现 |

Group 是任务与结果汇总边界，Unit 是处理机会和一轮尝试的载体；二者不是一一对应关系。当前 Atomic 使用整组 Unit，Partial 先使用单 Pod Unit。混合粒度指有组成员的不同处理粒度，不包括独立 Pod 调度。

## 2. 模块进度表

“已实现”仅指表内列明的范围；“骨架”不代表能够运行完整机制。

| 模块 | 状态 | 已具备能力 | 尚缺内容 / 验收边界 | 主要代码 |
| --- | --- | --- | --- | --- |
| 启动与集群配置 | 已实现基础 | kubeconfig、typed/dynamic client、取消与退出 | 实际部署与集群版本验证 | `src/cmd/batchscheduler/main.go` |
| informer | 已实现基础及观察接线 | Pod/Node/PodGroup List/Watch、初始同步、lister；Pod/Group 事件直接通知 Observe | 持续事件到 Select/Transition 的业务反馈；异常回调重试及 Node 唤醒路径 | `src/informer/informer.go`、`events.go` |
| PodGroup CR | 已实现基础 | members、AllTogether / AllowPartial 模式、CRD schema | operator 创建及维护、CR status、声明变更协议 | `src/apis/scheduling/v1alpha1/types.go`、`config/crd/` |
| 组视图与成员关联 | 已实现 | namespace + UID 关联、成员索引、视图查询 | 删除与重建后的完整生命周期清理 | `src/group/view.go` |
| 组 / Pod 内部状态 | 部分实现 | 状态类型；进程内成员归属和部分观察更新 | 组级结果聚合、统一更新入口、过期结果校验、清理 | `src/group/state.go`、`src/workqueue/select.go` |
| Unit 生命周期 | 部分实现 | 组装、冻结、首次入队；Accept 登记 Processing，交接确认后登记 Done | 实际 Pop 交接凭据、取消及下一轮归属衔接 | `src/workqueue/unit.go`、`state.go` |
| 四个队列存取 | 已实现基础 | Active/FastRetry FIFO；Fail/Retry 按键存取 | 多队列消费、迁移一致性、等待唤醒、公平性 | `src/workqueue/active.go`、`fail.go`、`retry.go` |
| Select 统一入口 | 已实现当前范围 | 来源识别、粒度选择、查归属、组装、冻结、直接 Add/Put | 持续事件接入、缓冲区归属交接、恢复后回送 | `src/workqueue/select.go` |
| 默认 Policy | 已实现首次入队规则 | Atomic 等整组；Partial 单 Pod；确认的内部异常进 Fail；未确定状态不投递 | 失败记录时效、恢复后的决策；不把节点资源检查放在这里 | `src/workqueue/policy.go` |
| Select 行为测试 | 已通过 | 12 组测试，含混合事件、重复投递、失效 UID、冻结及四队列分发 | 未覆盖真实调度、绑定、资源竞争或完整恢复闭环 | `src/workqueue/select_test.go` |
| Transition | 部分实现 | Fail → Retry；Retry → Active 显式唤醒、相关变化记忆、退避及缓存身份复查 | 相关事件筛选、定时器及自动唤醒；完整缓冲区交接仍未实现 | `src/workqueue/transition.go`、`retry_policy.go`、`retry_wakeup.go`、`transition_test.go` |
| 缓冲区及待确定区 | 接口及模拟闭环已实现 | 接收/观察/评估/Apply/Report/Release；版本校验、动作身份、未知结果保留、反馈阶段推进、确认后清理 | 出队交接协议、真实执行器和队列侧交接、FastRetry 准入及 Transition 接线 | `src/staging/` |
| compaction | 讨论方向 | 已确定放在缓冲区侧讨论 | 合并资格、组边界、旧尝试确认结束、新 Unit 身份；不修改冻结单元 | 暂无实现 |
| 调度主循环 | 未实现 | Active 具备 Pop 方法 | 统一消费、尝试创建、取消与并发边界 | 暂无实现 |
| 实际调度周期 | 未实现 | informer 提供原始节点事实 | 资源快照、过滤 / 选择、组内联合资源计算、放置方案 | 暂无实现 |
| 调度资源缓存与 Assume / Forget | 未实现 | 已区分其与 informer 缓存的职责 | 临时占用、确认转正、失败释放、幂等与超时确认 | 暂无实现 |
| binding cycle | 未实现 | 仅有 PodBinding / PodBindingUnknown 等状态定义 | 真实绑定请求、异步结果、部分成功及未知结果处理 | 暂无实现 |
| 感知与组结果汇总 | 观察及评估部分实现 | Pod/Group 变化更新本轮观察；Atomic/Partial 恢复范围建议 | 原生错误映射、执行结果反馈、完整组进度及恢复闭环 | `src/staging/observe.go`、`evaluate.go` |
| operator | 未实现 | 约定创建 PodGroup 和带组 ownerReference 的 Pod | 创建、重建、业务生命周期维护 | 暂无实现 |

## 3. 当前实际可运行的路径

```text
集群配置 → 三类 informer → 初始缓存同步
                             ↓
                       main 遍历启动时的组
                             ↓
                  Group View → Select → Policy
                             ↓
                   冻结 Unit → 首次进入队列
                             ↓
                    尚无调度消费主循环
```

informer 会继续维护缓存，但持续变化尚未触发 Select。`Select(Pod 来源)` 已有接口和内存测试，尚未注册为 informer 事件回调。当前程序不会创建、绑定或删除 Pod。

已接通的并行观察路径：

```text
Pod / PodGroup informer 回调 → Observe → 更新已有 Unit 的最新事实
                                  ↓
                         提交去重的 Group Key
                                  ↓
                 后台 worker 分批取 Key → Evaluate
                                  ↓
                     Apply → 新成员 Select / 注入动作适配器
```

Accept 也会提交评估提示，但 Active 出队到 Accept 尚未接线；无本地 Unit 时仍可评估未持有的新成员，暂不保存第二份长期组缓存。

## 4. 尚未闭合的关键位置

| 位置 | 现状 | 后续需要保证 |
| --- | --- | --- |
| Pop 后的生命周期 | Pop 后仍为 Queued，Accept 成功登记 Processing；交接结束为 Done | 完善实际消费和交接失败协议，不能丢失或重复处理 |
| 队列间迁移 | Fail → Retry、Retry → Active 已实现，其他路径返回“尚未实现” | 统一入口串行管理；原始队列 Get 不是跨队列快照，后续消费者须接入管理协议 |
| 缓冲区交接 | Accept 已登记本地归属，尚未接入消费流程 | 当前来源状态不证明已实际出队；完整交接和 workqueue 生命周期更新仍待实现 |
| 失败判定 | LastFailure 非空可触发 Fail | 区分历史错误与当前尝试，确认未知结果后再决定恢复 |
| Atomic | 已实现整组齐备、整组入队 | 还未实现整组资源规划、预留、提交及失败协调，不代表跨 Pod 原子绑定 |
| 恢复回送 | 模拟执行器可恢复并确认交接，真实 Handoff 适配器未实现 | 队列侧原子解除旧归属、组织新轮、经 Select 接收；包含未失败成员的责任交接 |
| 重启 / 删除 / 声明变化 | 内部记录主要为进程内数据 | 过期记录清理、从集群事实重建、旧 UID 和旧尝试结果隔离 |
| 并行调度器接管范围 | 当前只按组归属过滤 | 真正提交绑定前明确默认调度器与本调度器的接管协调方式 |

这些是阶段性缺口，不在本次文档整理中提前实现。

## 5. 接下来的开发顺序

| 顺序 | 工作 | 本阶段先定什么 | 阶段验收 |
| --- | --- | --- | --- |
| 已完成基础 | **Transition** | 两条迁移的校验及显式提示路径已实现，实际事件筛选和定时器待后续接入 | 当前范围内不丢单元、不重复；非法迁移不破坏原位置；FastRetry 后置 |
| 1 · 当前 | **Center 适配与交接** | Apply/Report/Release 的模拟闭环已通过；下一步讨论并逐个接入真实执行器和 workqueue 交接 | 实际交接不丢旧归属、不重复投递；无适配器或未知结果不提前释放 |
| 3 | 调度主循环与实际调度 | 从 Active 取出 Unit；按受限资源规则计算 Pod → Node 方案 | 内存场景能做出整组 / 单 Pod 方案，区分可调度与不可调度 |
| 4 | 调度缓存与 Assume / Forget | 记录本地方案占用，后续尝试可见；失败和确认结果如何更新 | 异步绑定前不重复使用本调度器已预留的资源，重复释放安全 |
| 5 | binding cycle | 执行方案、异步反馈、成功 / 失败 / 未知结果进入缓冲区 | 能对真实 Pod 提交绑定，并与 informer 事实确认，不把请求返回当作容器已运行 |
| 6 | 感知与反馈闭环 | 按组汇总；Atomic 组级、Partial Pod 级恢复；等待和重试唤醒 | 一次首次调度和一次失败恢复都能完整走通 |

开发顺序不是运行时顺序。真正执行时，Assume 必须参与绑定前的资源记账，不能等异步绑定已经并发运行后才补上。感知与缓冲区可以先用模拟结果验证，不要求一次写完整个调度器。

compaction 在缓冲区基本归属与反馈规则明确后单独讨论；FastRetry 准入、公平性、Agent 旁路和更多策略继续后置。

## 6. 本次测试记录

已执行并通过：

```sh
go test ./src/... -count=1
```

- 12 组测试，包含 Atomic / Partial 与 Pod / Group 来源组合、首次入队和重复事件。
- 并发混合场景：24 个组、96 个 Pod、240 次事件，验证无重复成员和跨组混入。
- 验证 Atomic 成员到齐后复用组装单元；Partial 不必等待整组。
- 验证无组 Pod、旧 Pod / Group UID、已绑定和删除中的成员不会作为新任务投递。
- 验证已确认内部异常的 Fail 粒度，以及预留、绑定中和绑定结果未知时不入 Active。
- 四队列分发使用注入策略验证，不代表默认 Policy 已实现 Retry / FastRetry 准入。
- Center 接收、观察、评估有内存测试，fake informer 覆盖绑定 / 删除事件的回调链路；整体缓冲区闭环未完成。
- 评估测试覆盖在途 / 未知结果等待、Atomic / Partial 失败范围、声明变化、绑定后结清门槛及无本地 Unit 的观察。
- 评估队列测试覆盖重复 Key 去重、处理中再次通知及防止 worker 自刷新造成忙循环。
- Apply 测试覆盖模拟正常/失败闭环、重复并发决策、迟到反馈、未知绑定和未知交接、账目结清、缺少适配器不清理；组级 Apply 已与真实内存 workqueue.Select 联测。
- 发现并修复组装期间同名 Pod 重建导致旧 UID 冲突的问题，补充回归测试。

上述测试不连接真实集群，不是性能压测、竞态检测或调度成功率验证，也未覆盖实际调度 / binding / Assume。

## 7. 文档与当前决策

背景需求说明书 v0.2 中的“纯 Group 队列”和 schedulerName 接管描述属于较早方案。本阶段已经采用弹性 Unit、组 / Pod 混合处理粒度，并暂按组归属过滤；以后进入真实绑定时再落实接管协调。当前实现状态以本表和代码为准，需求说明书不代表功能已经实现。

2026-10-19：实现 Fail → Retry 并补充分类、前置条件和幂等测试。`RecoveryComplete` 是恢复路径提供的明确确认，不表示当前已实现实际回滚；Transition 仍会拒绝内部记录为绑定中、已绑定、结果未知或仍由暂存区持有的成员。资源/节点/依赖类等待相关变化；临时调度错误或已确认的绑定拒绝等待明确退避时间。未知错误、声明错误、权限错误、对象删除及恢复未完成不自动重试。Retry → Active 已支持显式 RetryWake：匹配相关变化、记住退避期间事件、到期后重查缓存与归属并激活；持续事件接线、候选索引和定时器尚未实现。

后续仍保持先共同确定规则、再实现和验证的节奏，不一口气补完 Center 或绑定实现。

2026-10-19：新增 src/staging 接口骨架。Accept（队列出队）与 Observe（感知层观察）是并行入口；Observe 不要求预先存在本地 Unit。Report 接收本轮执行反馈，Evaluate/Apply 分开决策与执行，Release 定义责任结清后的释放边界。仅有接口、数据结构与注释，尚无缓冲区实现或运行接线。

2026-10-20：实现 Buffer.Accept 与 ContainsPod。独立 Unit 记录通过组索引关联；Partial 允许不重叠单元并存，Atomic 不允许未释放时接收另一轮。同键同成员提交幂等，冲突不会覆盖旧记录或留下部分索引。仅接收登记，不启动调度、不清理旧项，尚未接 main 或完整实现 Center。staging/workqueue 包测试通过，覆盖并发归属、缓存变化、幂等和非法准入。

2026-10-20：实现 Observe，main 注册 Pod/PodGroup informer 回调，直接通知 Center 重读 Store 更新本轮 Observed 事实。与内部执行状态分开；不自动映射调度失败、释放归属或回送。无本地 Unit 的观察不创建执行记录；后续 Evaluate 仍从 Store 取组信息。已用 fake informer 验证绑定/删除事件到缓冲区的实际回调链路，staging/informer/workqueue 测试通过。真实集群未测试，回调失败目前只记录日志，重试及反馈闭环待后续实现。

2026-10-20：实现 Evaluate 和独立评估 worker。Accept/Observe 提交 Group Key，按组去重并以批量上限消费；处理期间的新通知会再次评估，worker 自身刷新不会形成忙循环。按本轮状态输出等待、开始调度、恢复、回送 Fail、释放建议；Atomic 扩大失败范围，Partial 保留成员范围。新增明确的恢复完成/责任结清字段供后续 Report 填写，缺省不允许清理。main 缓存就绪后启动 worker，目前只接收决策，Apply/Report/Release、实际队列交接及队列侧 Transition 接线均未实现；错误当前记录日志并等待下一次通知。go test ./src/... -count=1 全部通过，未测试真实集群。

2026-10-20（续）：完成 Apply 翻译/分发，补最小 Report、Release 和未知交接确认。按 Record 版本及 ActionID 防重复和迟到反馈；操作在途或账目未结清时保持归属。成功交接后删除 Unit、成员索引及空组索引，旧 Unit 标为 Done。完整模拟正常路径及 Atomic/Partial 失败恢复路径通过；真实队列回送及旧归属清理适配器未实现，不能视作实际调度闭环。main 仅接入新成员 Select；未注入 Execute/Handoff 时保留记录并报明确错误。compaction 和 FastRetry 策略未展开。
