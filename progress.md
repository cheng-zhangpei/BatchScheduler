# BatchScheduler 架构与进度

更新日期：2026-10-29。以当前代码和本次讨论为准，不使用百分比估算完成度。

### 2026-10-29：Active → Buffer 已接通，准备真实集群验证

- 新增 `cmd/batchscheduler/active.go` 的 runActive：持续 Pop，再调用 Accept。Accept 自己通知评估，无需消费者直接触发 Observe 或 Execute。
- main 在缓存同步后启动消费者；接收失败时 Unit 放回 Active，错误交给 main 退出，不立即重试过期 Unit。自动重组尚未实现，当前保留这一明确退出策略。
- 退出时取消上下文并等待消费、评估、执行和 informer 结束，再关闭历史数据库。FastRetry 消费仍未启用。
- 验证通过：`go test ./src/cmd/batchscheduler -count=1 -timeout=60s`；`go test -race ./src/cmd/batchscheduler -run TestActiveConsumer -count=1 -timeout=60s`。覆盖不手动 Pop/Accept 即自动执行 Bind、Accept 拒绝后放回队列、Pop 后取消不丢 Unit，以及空队列等待时正常退出。
- 消费函数及接线已经过用户查看。下一步启动虚拟机集群，验证真实 Pod 的入队、选点、Assume、Binding、Informer 确认及清理；目前尚未进行真实集群测试。

以下保留之前的开发记录，当前状态以顶部记录为准。

### 2026-10-29：显式 Inflight 队列与真实 Bind API

- 修正上一版“调度后直接开 goroutine”的结构：Run 串行完成 Assume 后，只调用 Inflight.Submit；独立 Inflight 保存待绑定任务，并用固定 worker 调用 runBinding → steps.Bind → Report。
- 新增启动参数 `--max-inflight`、`--bind-workers`、`--bind-timeout`，分别控制总轮数、实际绑定并发和每轮 API 等待时间。容量在调度前取得，worker 完成后归还，参数在连接集群前校验。
- steps.Bind 按冻结成员和 Plan，携带 Pod UID 调用 client-go Pod binding API，一轮内顺序提交。main 已传入真实客户端。
- API 成功经 Report Bound 回 Buffer；观察到实际绑定后，可以 Release、归档、清理。已测试实际占用接替暂占时不重复累计资源。
- 明确拒绝且没有成功成员时 Recover → Forget → Fail，临时绑定拒绝再由现有错误引擎生成带退避的 Retry 决策。未知结果或部分成功保留记录，不虚报整轮已回滚；其查询对账、Atomic 回滚和 Partial 分别交接仍待实现。
- 新增测试覆盖真实 client-go HTTP Binding 请求、单 worker 排队、可配置上限、后台阻塞时后续调度和资源可见性，以及 Buffer 成功清理和错误回送。详细源码说明见 `docs/binding.md`。
- 验证通过：全量 `go test ./src/... -count=1 -timeout=90s`；新增绑定、Inflight 和并发场景通过 `go test -race ./src/scheduler/executor ./src/scheduler/steps ./src/cmd/batchscheduler -run 'Test(Inflight|AsyncBinding|Bind|Binding|PartialBind)' -count=1 -timeout=90s`。
- main 的 Active/FastRetry → Accept 消费循环仍未启用；未连接真实集群，不能把上述执行线验证当成完整系统部署验证。

以下保留之前的开发记录，当前状态以顶部记录为准。

### 2026-10-29：先拆出异步 Binding 骨架

- 串行 `schedule` 运行到 Assume 和 Report Assumed；成功后交给后台 `runBinding`，串行线程可以继续下一轮调度。
- 后台继续使用同一个 Attempt 和 Report。Buffer 无需新接口，也不负责启动或监控每个绑定步骤。
- 同时进行的调度/绑定最多 4 轮，名额在调度前取得；后台结束后释放。同一次请求在后台期间保持去重标记，Run 退出时等待后台协程结束。
- 本步没有实现真正的 API 绑定。Bind 仍返回未开始，后台沿原路径 Recover → Forget → Blocked；绑定结果未知时的对账仍待实现。
- 测试通过：`go test ./src/scheduler/executor ./src/cmd/batchscheduler -count=1 -timeout=60s`，以及新增异步用例的 `go test -race ./src/scheduler/executor -run TestAsyncBinding -count=1 -timeout=60s`。验证后台暂停不阻塞下一轮、下一轮看得见暂占、请求去重、并发上限和退出等待；未连接集群。

以下保留之前的开发记录，当前状态以顶部记录为准。

### 2026-10-27 收工：Assume / Forget 已接入执行线

- 已接 `steps.Assume`：用本轮成员与最终 Plan 组装 Assumption 批次，调用共享缓存，成功后立即标记 Reserved，再由 Executor Report Assumed。
- 已接本轮未提交绑定时的 `steps.Recover → Cache.Forget`。绑定结果未知、已发出绑定或新的恢复请求接手旧执行时，不会用未经确认的身份清理资源，这些恢复路径仍待实现。
- 当前正常测试路径：Filter → Pod Score → PlanGroup → Assume → Assumed → Bind 占位确认未开始 → Recover/Forget → Blocked/Recovered（已结清）。没有真实绑定，也不把待实现 Bind 当成业务失败重复入队。
- Assume 资源/端口复核失败映射为内部 Unschedulable，节点消失映射为 NodeUnavailable。已验证错误经 Report → Evaluate → 回送 Fail，再通过 Apply/Transition 进入 Retry；不是所有错误都自动重试。
- Report 调用失败仍保留资源责任，不当成明确绑定失败去 Forget；失败对账和接手旧执行的资源身份协议尚未实现。
- 验证通过：全量 `go test ./src/... -count=1 -timeout=90s`；随后补充的两个回归用例也通过，分别验证 Assumed 报告失败不误退资源、新恢复请求不能清旧执行的暂占。未连接真实集群，未做性能压测。
- 下次只需先 review 本轮接线，再讨论 Bind。Active/FastRetry 消费、真实绑定与绑定后恢复、其他语义评分和生产级清理仍待逐个实现。

以下保留开发过程记录，当前状态以顶部收工记录为准。

### 2026-10-27：按执行身份批量 Forget

- 实现 `Cache.Forget(UnitKey, ActionID, []PodRef)`：按 UID 查询暂占，核对所属执行，使用保存的 Pod 和节点退账，再删除 pods、assumed 记录。
- 使用节点副本先退账、再统一提交，归属不一致时整次不变；重复调用、重复成员不会重复退资源，已确认绑定的占用不会被撤销。
- 验证通过：`go test ./src/scheduler/resourcecache ./src/scheduler/steps ./src/cmd/batchscheduler -count=1 -timeout=90s`。覆盖部分成员释放、资源和 HostPort 释放、旧快照不变、归属错误不误清理、确认绑定保留、节点删除后的遗留账清理，以及 Assume → Filter 拒绝 → Forget → Filter 恢复候选。
- 仍只接通缓存方法。steps.Assume、Recover 的调用及错误分类尚未接入，执行线继续停在 Assume，没有提交真实绑定。下一步再接这两端。

以下保留先前检查点，当前状态以顶部记录为准。

### 2026-10-27：资源缓存的批量 Assume

- 实现 `resourcecache.Cache.Assume([]Assumption)`，一批只接收同一 Unit、同一 ActionID。输入 Pod 已带目标 NodeName，缓存保存独立副本。
- 在最新资源账的受影响节点副本上累计检查资源与 HostPort，全部通过后一起更新 nodes、pods、assumed；任何成员失败都不留下本次部分占用。同一执行的相同计划重复调用不重复计费，其他执行或改变旧计划会被拒绝。
- 测试已改为真实调用 Cache.Assume，验证无 informer 绑定通知时下一轮 Filter 仍看见暂占；另覆盖旧快照不变、确认绑定不重复计费、失败不污染原账和两个批次不能重复消费资源。
- 验证通过：`go test ./src/scheduler/resourcecache ./src/scheduler/steps ./src/informer ./src/cmd/batchscheduler -count=1 -timeout=90s`。未连接真实集群。
- 只实现缓存方法，steps.Assume、Forget 和执行恢复仍待逐个接入，未启用执行线的资源暂占，也未实现绑定。下一小步先补 Forget，再接执行入口。

以下保留先前检查点，当前状态以顶部记录为准。

### 2026-10-27：Filter 改读共享资源账

- main 将 informer 更新的同一份 ResourceCache 传给 Executor → Runner。Filter 从共享账取本轮快照，不再使用 Node/Pod lister 重建资源占用；成员与组声明仍由 GroupView 提供。
- 增加原生 Filter 的 NodeInfo 快照入口，继续调用原来的六个插件，并保留既有 Pod 硬反亲和性的未支持检查。旧 Node/Pod 构造入口保留给独立适配测试。
- 资源快照同时给出包含全部占用的 Nodes 和仅含真实绑定的 BoundPods。暂占消耗资源，但不能被当成已经运行的同组成员。Score 与组试放继续沿用 Filter 的固定快照。
- 本轮没有实现 Assume 登记或 Forget；缓存支持的读取路径先接通，再逐个实现资源变更方法。主线仍停在 Assume。
- 验证通过：`go test ./src/scheduler/... ./src/informer ./src/cmd/batchscheduler -count=1 -timeout=90s`。新增检查共享占用影响下一轮过滤、已开始轮次快照不变、资源释放恢复候选，以及暂占与真实绑定成员的区分；暂占记录由测试直接构造，不代表 Assume 已实现。

以下保留先前检查点，当前状态以顶部记录为准。

### 2026-10-27：首次 informer 同步建立资源账

- main 在启动 informer 前注册资源记账。Node 事件维护容量和属性，已绑定 Pod 事件维护资源与端口；包含其他调度器和 namespace 的 Pod，未绑定 Pod 不计入节点占用。
- 增加按 Pod UID 查询的已记账对象索引，用保存的旧对象撤销占用，再记入更新后的对象。支持 Pod 先到、Node 后到，不重复累计同一 Pod。
- ready 同时等待 informer 缓存和资源记账处理器的初始 HasSynced；初始记账未完成或失败，不启动执行与评估线程。后续事件继续更新资源账。
- 新增只读 NodeInfo 快照用于查看和验证；Filter 尚未改读这份共享账，Assume/Forget 及真实资源恢复仍未实现，本轮没有修改 Buffer 的状态职责。
- 验证包含：初始回调阻塞时 ready 不开放、其他调度器已绑定 Pod 的占用、Pending 不占用、Node/Pod 先后顺序、重复通知、资源与端口更新、删除，以及快照不改变共享账。
- 检查通过：`go test ./src/informer ./src/scheduler/resourcecache ./src/cmd/batchscheduler -count=1 -timeout=90s`；另单独验证初始化记账失败时不开放 ready，并能停止 informer。未连接真实集群。

以下保留先前检查点，当前状态以顶部记录为准。

### 2026-10-27：Assume 先定义资源缓存

- 新增 `scheduler/resourcecache`，只定义 Cache、Assumption 和空缓存构造函数。
- Cache 用节点名索引 NodeInfo 总账，用 Pod UID 索引暂占明细。Assumption 保存 Unit、ActionID 和带目标节点的 Pod 副本，供后续确认绑定和 Forget 使用。
- 本轮未实现资源变更方法，也未接入 informer、Filter 或 Executor；运行路径仍停在 Assume。下一步逐个实现初始同步、快照、Assume 和 Forget。

以下保留先前检查点，当前状态以顶部记录为准。

### 2026-10-27：首版 selectBest 接回执行线

- 只实现计划选定：沿用通信搜索排序，取第一套完整方案，核对本轮成员及节点存在性，再返回独立位置表。累计资源与端口检查仍由 Place 负责，不重复搜索。
- 现有其他语义评分接口保留；多语义之间的比较规则还没定，存在额外集中分时仍返回待实现错误，不忽略分数、不擅自分配权重。
- 执行线现在可从 Scored 推进到 Planned，经 Report 把计划交回 Buffer；随后停在尚未实现的 Assume。没有真实资源预占和绑定，Active 消费也仍待实现。
- 验证通过：`go test ./src/scheduler/... ./src/cmd/batchscheduler -count=1 -timeout=90s`，含计划选定及独立副本、不完整方案拒绝、多语义待实现和真实执行线的 Planned → Blocked（Assume）反馈。
- 下一个方法单独讨论 Assume；不顺带实现资源缓存、Bind 或新的语义评分。

以下保留先前检查点，当前状态以顶部记录为准。

### 2026-10-27：通信引导搜索，先接 Priority

- 沿用现有框架：trial 保存部分方案，placeLink 扩展关系，keepTrials 按层裁剪；通信引导搜索，其他语义评价完整候选。
- 本轮只改一个行为：resolveLinks 按 Priority 从高到低排序，同值按名称，不修改输入声明。现有降级比较因此先保护重要关系。
- 保留不同布局及部分更远方案的做法不变，给后续集中评分留选择；这不保证保留所有有价值的候选。
- 新增同优先级按名称保护关系、同时保留另一种妥协方案的检查；此前失败的优先级用例一并复查。
- 验证通过：`go test ./src/scheduler/... ./src/cmd/batchscheduler -count=1 -timeout=90s`，覆盖通信搜索、原生适配、执行编排和现有入口集成；没有做真实集群测试。
- 集中评分公式、最终偏重与选定、跨 Unit 端点和真实资源操作仍待逐步实现，本轮没有增加通用搜索框架。

以下保留历史检查点，当前状态以顶部记录为准。

### 2026-10-26 晚：只补做法注释，原型待逐步 review

- 对 Place、链路解析、节点对枚举、累计资源试放、候选去重与布局保留补充具体做法和例子；本轮不扩展算法。
- **当前已知失败：`TestPlaceAccumulatesSharedEndpointAndDegradesLowerPriority`。resolveLinks 当前按 YAML 顺序处理，没有 Priority 排序；本次按用户选择保留逻辑，标记测试失败，不顺手修复。**
- 前一轮的全量通过记录是当时的检查点，不代表当前优先级行为已经正确。当前验证结果以本节及本次实际输出为准。
- 今晚复查 `go test ./src/... -count=1 -timeout=90s`：仅上述优先级用例失败，其余包通过；注释编辑前后 Go 代码 token 一致，没有变更算法逻辑。
- 明天先从两个 Pod 同节点的最小场景开始，再加共享端点的第二条链路，逐步推导关系顺序、累计资源、降级和候选裁剪。
- 详细阅读顺序与待推导问题见 [通信放置插件源码说明](docs/communication-placement.md)。这份代码先作为 checkpoint 备份，不要求一次接受所有设计。

以下是本日先前的实现记录。

### 2026-10-26：通信放置主线，先 review 再推进

- PodGroup 增加 placement 声明，链路引用稳定 member-id；CRD、Go 类型和示例同步。
- Select 为 Unit 保存组声明快照，冻结后不变；Buffer、Execution、Score 将 Placement 副本交给插件。旧声明的自动队列重组仍未完善。
- planner 定义候选方案和插件接口，plugins/communication 实现本轮成员的候选生成；可通过 Executor.SetPlanner 替换插件。
- Place 按链路优先级沿 NodeScore 列表试放，累计检查原生资源与 HostPort；共享端点不重复计费，冲突时尝试更远的拓扑范围。
- 默认保留最多 4 套方案，并保留不同分布形状；补齐未参与链路的本轮成员。有限搜索失败不等于证明全局无解。
- 原始评价暂用 Pod 基础分求和与通信 weight × 靠近分；候选另保留逐条规则、实际等级和说明。
- 集中评分、最终归一化与选定、跨 Unit 端点、真实 Assume/Bind、Active 消费仍未实现。Place 产出候选后，执行线仍在最终汇总报告 Blocked。
- 此阶段的 `go test ./src/... -count=1 -timeout=90s` 曾通过；晚间复查发现优先级排序缺失，当前失败记录见顶部。没有真实集群、网络性能或大规模搜索压测。
- 阅读入口：[通信放置插件源码说明](docs/communication-placement.md)。下一步先逐函数 review Place → placeLink → trial.place，由用户决定后续实现，不直接扩展所有算法。

以下保留此前检查点，以本节为最新进度。

### 2026-10-25：成功归档与跨 Unit 组信息

- 成功 Release 在队列交接和内存清理前写入 BoltDB；写失败保留 Record。失效清理不存成成功记录。
- Record 增加独立 ID，数据库按组 UID 和 Record ID 隔离，避免重启后的数字 Unit 编号覆盖历史。
- GroupContext 查询其他活跃 Unit 和成功历史；PlanGroup 另收到当前绑定的 BoundPeers。历史不冒充当前位置，也不重复计资源。
- main 已接本地库和读取器，默认 `.batchscheduler/history.db`，支持 `--history-db`。
- 本次不强制合并冻结 Unit；真实 compaction、运行期失败回调、未完成操作恢复、历史过期清理仍待讨论。
- `go test ./src/... -count=1 -timeout=90s` 通过；新增关库重开、写入失败、先保存后交接、组 UID 隔离与规划输入测试。
- 源码说明见 [组上下文与成功记录](docs/group-context.md)。下一步再讨论通信语义。

### 2026-10-25：补齐组规划输入，算法待讨论

- GroupScoreInput 现在包含 Mode、完整 Pods、NodeInfos、各 Pod 的候选评分列表。
- Pods 使用本轮成员的副本；NodeInfos 从 Filter 固定的节点视图导出，保留已有占用和拓扑，不重新读缓存。
- NodeInfo 的资源统计可独立试放；它引用的已有 Pod/Node 对象只读，修改前须 DeepCopy。
- 本次只补输入与说明，不实现组评分、组内试放或组选点算法；PlanGroup 仍保留 TODO。

### 2026-10-23：接入 Pod Score，组评分待讨论

- 执行链为 Filter → Pod Score → PlanGroup → Assume → Bind。Mode 从冻结 Unit 经 Execution、GroupScoreInput 传入组规划。
- upstream.PodScore 已接 NodeResourcesFit（LeastAllocated）、NodeResourcesBalancedAllocation、NodeAffinity、TaintToleration，四插件等权；CPU/内存等权。
- PreScore / Score / NormalizeScore / 汇总均调用原生实现；评分复用 Filter 的同一份 NodeInfo。
- 每个 Pod 返回总分降序列表和插件分数，保留全部候选；Atomic/Partial 的差异暂不作用于 Pod 基础评分。
- Report 增加 Scored 进度；组评分、组内试放和组选点尚未实现，当前在 PlanGroup 转为 Blocked。
- 评分适配、真实插件和 mode 透传已有测试；没有集群或性能测试。阅读入口见 [Pod Score](docs/score.md)。

下次从组评分和组选点开始讨论：输入已是 Mode + 各 Pod 的完整评分列表，入口为 `src/scheduler/steps/plan.go`；先确定偏好与规则，再写实现。

以下保留本日先前的开发记录，以本节为最新评分进度。

### 2026-10-23：先修正 Partial 的 Unit 组装

本日执行线新增：`steps.GroupFilter` 已通过 `upstream.PodFilter` 实际调用六个原生基础过滤插件。
源码子集固定在 `third_party/kubernetes`，v1.34.7，21 包 / 54 个 Go 文件约 1 MB；原生 Go 文件未修改，带许可证和 SHA-256 清单。
适配层读取本轮 Node/已分配 Pod，准备原生 NodeInfo，为每个成员执行 PreFilter/Filter，返回候选节点和拒绝原因。
这不是完整默认过滤配置；硬 Pod 亲和性、硬拓扑、存储与 DRA 尚未接入，会明确报错。

执行边界已调整：Buffer 只提交整轮 Run，Executor 快速接进内部队列，由一个执行工作线程组织 Filter → PlanGroup → Assume → Bind，失败时内部 Recover。
同一轮通过同一个 Report、同一个 ActionID 持续回报。Progress/Unknown 保留 Pending，Finished 且资源已结清才结束等待；Evaluate 不再派发 Assume/Bind。
当前 Filter 已实现；组选点未实现时报告 Blocked，停止等待开发，不自动重新执行。过滤失败且尚无资源占用时可以内部结清，再回 Fail。

目录已拆分：
- `src/scheduler/upstream/`：原生插件适配；`executor/`：编排和回报；`steps/`：具体步骤。
- `src/staging/`：共用类型和接口；`buffer/`：记录和交接；`evaluation/`：分阶段决策；`error_engine/`：错误映射和规则。

**目前已有过滤能力、执行工作线程和连续回报；组内节点选择、真实 Assume/Bind/Recover、Active 消费仍未完成。**
我们只需要先掌握调用边界，说明见 [Filter 适配](docs/filter.md)，不要求逐行阅读第三方内部实现。

- Partial 不再默认每个 Pod 一个 Unit。在 assemble 中查询 Buffer.ContainsPod，排除在处理的成员和已有归属的成员，把当前可接收的同组成员批量装进尚未冻结的 Unit。
- Pod 事件仍保留触发来源信息，但组装会检查同组其他成员；不等整组到齐，不追加到已入队/进入 Buffer 的旧 Unit。
- 已有明确失败的成员与正常成员分开组装。本次只修正组装行为，不宣称整操作反馈已经支持完整的成员级成功、失败和回送。
- 后续仍按一个接口、一个决策逐步接入。以下旧检查点中“Partial 单 Pod”的描述由本节更新。

本日随后实现 Handoff 的 Release 分支：`WorkQueue.ReleaseUnit` 只接受 Processing/Queue=None，
删除旧 Unit、组内编号和仍属于它的成员状态；保留同组其他 Unit，旧记录已不存在时重复调用成功。
main 已接 Release，Buffer 在队列清理成功后再标记 Done、删除 Record。

随后接通 Handoff 的 Return 分支，统一调用 `WorkQueue.Select`，没有增加第二个公开入队入口：
- `SelectRequest.Return` 提供旧 Unit、回送成员、内部错误及恢复完成确认。
- Atomic 回送整轮；Partial 可以把多个失败成员放进同一个新 Unit。旧 Record 删除后会重新通知评估，剩余未失败、未绑定的成员走普通 Select，不跟着失败成员进 Fail。
- 先检查并准备新 Unit，再替换旧队列记录、把新 Unit 放入 Fail；函数成功后 Buffer 才标记旧 Unit Done 并删除 Record。
- 已验证模拟执行失败 → 真实 Handoff/Select → Fail → Evaluate/Apply/Transition → Retry → 相关事件 → Active → 再次 Accept。真实执行器和消费循环仍未实现。

### 2026-10-21 收工位置 / 明天回来先看这里

今晚先读代码，不继续扩展功能。明天询问昨天进度时，先复盘下面的 Evaluate 阶段与缺口；
每次只走一个阶段、一个 Unit，不把“接口已接线”说成“真实执行已完成”。

| Evaluate 阶段 | 当前做到哪里 | 下一步需要确认什么 |
| --- | --- | --- |
| 预处理 | 在途等待、观察不足、本轮声明/成员失效检查；只生成决策 | 超时确认及完整生命周期异常尚未实现 |
| 报喜 | 成员已绑定且责任结清则生成 Release；不在评估里删记录 | 真实清理仍需 Handoff 解除队列侧旧归属 |
| 报忧：Center 中的 Unit | 错误引擎集中入口，case 分类到 handler；Observe 已匹配 Unschedulable、SchedulerError、SchedulingGated 三种标准原生原因；已有错误先恢复、再回 Fail | 其他原生原因、API 请求错误及错误属于哪一轮的完整判断仍需补；各类错误策略还需逐项细化 |
| 报忧：Fail/Retry 中的 Unit | 读取队列快照，按错误、恢复确认和唤醒条件生成 Transition；Apply 已调用真实迁移 | 回送者必须明确确认 RecoveryComplete，不能仅因任务在 Fail 就准许重试 |
| 正常推进 | 只生成一次 Run；Executor 内部组织各步骤并持续 Report | Filter 已接真实原生插件，组选点和真实资源操作待实现 |

详细说明见 [Buffer 源码阅读说明](docs/buffer.md)，逐个解释字段、函数、Handoff 和未完成项。

阅读顺序：`src/staging/evaluation/record.go` 的阶段入口 → `src/staging/error_engine/` 的错误 handler
→ `src/staging/buffer/apply.go` 的动作分发。`Evaluate` 不修改 Buffer，Apply/Report 等入口负责更新。

本日额外接通：Transition 队列版本检查；低频时钟、节点变化及已绑定 Pod 删除的重试通知。
测试无需手写 Decision，已验证评估器自动生成并执行 Fail → Retry → Active；不相关事件、
未恢复、权限/声明错误及绑定未知不自动重试。`go test ./src/... -count=1` 全部通过。
调度门移除已产生 DependencyChanged；其他 DependencyNotReady 依赖资源的 informer 尚未接入，没有做真实集群验证。

后续协作方式：一次一个函数、一个决策，由用户主导行为和取舍，再讨论实现；不再整段放权扩写。
本日后半段重点整理直白注释与源码文档；另外按用户明确要求补上 Observe → 内部错误 → Evaluate/error_engine 的三种标准原因映射。

**仍未接入的不是所有 handler：Select、Transition 已接通；Execute 真实执行器、Handoff
真实回送/归属解除适配器，以及 Active.Pop → Accept 消费主循环仍未完成。**

以下 2026-10-20 收工记录和模块表保留为上一检查点；本节描述优先。

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
| 调度资源缓存与 Assume / Forget | 本地执行闭环已接通 | informer 初始化与持续校正、本轮资源快照、批量暂占、确认转正、按执行身份撤销及步骤接入 | 已提交/未知绑定恢复、旧执行接手、Report 失败对账及超时确认 | `src/scheduler/resourcecache/`、`src/scheduler/steps/` |
| binding cycle | 未实现 | 仅有 PodBinding / PodBindingUnknown 等状态定义 | 真实绑定请求、异步结果、部分成功及未知结果处理 | 暂无实现 |
| 感知与组结果汇总 | 观察及评估部分实现 | Pod/Group 变化更新本轮观察；Atomic/Partial 恢复范围建议 | 原生错误映射、执行结果反馈、完整组进度及恢复闭环 | `src/staging/buffer/observe.go`、`evaluate.go` |
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
