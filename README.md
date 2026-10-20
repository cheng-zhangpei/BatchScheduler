# BatchScheduler

参考 Kubernetes 调度器结构，从零实现以 Group 为单位的调度核心。每次只推进一个小目标。

**整体调度流程尚未打通。当前正在开发 Center，已接通观察和评估 worker；动作执行、实际调度、Assume / Forget 和 binding cycle 均未实现。** 架构边界、模块进度、测试范围与后续安排见[项目进度表](progress.md)。

当前已建立 PodGroup CR、Pod 成员视图及关联。`src/workqueue/` 保留 `Unit` 接口及一个默认实现，同一个单元从组装走向冻结；四个队列均以 Unit 存取。main 在初始同步后调用 `Select(SelectRequest{View: view, Nodes: nodes})`，查询 Pod 归属、复用或创建单元，并登记组内关系。默认策略为 Atomic 整组齐备后进 Active，Partial 按单 Pod 入 Active，已确认的内部异常进 Fail，绑定结果待确定时不入队。Select 直接首次入队；队列间迁移由 Transition 管理，已实现 Fail → Retry 及显式提示驱动的 Retry → Active。

`src/workqueue/transition.go` 已实现 Fail → Retry：校验失败分类、恢复确认、当前归属及成员状态，保存结构化重试计划；相同请求重放不会重置退避时间。`retry_policy.go` 注释列出可重试类型、不能直接重试的情况，以及Retry → Active 的事件/时间门槛。`retry_wakeup.go` 已实现显式提示驱动的激活及状态复查，持续事件和定时器尚未接入，FastRetry 不参与本阶段迁移。缓冲区确认的失败经 Select 回送，不走 Active → Fail。

`src/staging/` 已实现 Center 的接收登记、观察更新和评估。Accept 保存 Unit 记录、同组索引及成员归属；Observe 从 Store 重读当前事实；Group Key 去重队列连接独立评估 worker，按组产出等待、调度、恢复、回送及释放建议。main 已接上 Pod/PodGroup informer → Observe → 评估 worker，当前仅接收决策，尚未实现 Apply / Report / Release 或实际队列交接。

首次入队统一通过 `Select(SelectRequest)`，只处理有 PodGroup 归属的成员。Atomic 的 Pod 来源提升为组级，Partial 保留 Pod 粒度；`StagingLookup` 查询暂存归属，避免重复组装。Active.Pop → Accept 消费交接、持续事件到队列的反馈、资源缓存与 Assume / Forget、调度算法、绑定、operator 尚未实现。compaction、FastRetry 严格准入和 Agent 接入继续后置。

2026-10-20 收工：下一步逐个讨论并实现 Center 的 Apply、Report、Release，先用模拟执行器验证闭环。最近 `go test ./src/... -count=1` 全部通过，尚未测试真实集群。

```text
go.mod                         Go 模块，依赖 client-go 基础库
src/
  cmd/batchscheduler/main.go    配置与启动入口
  informer/                    Pod/Node/PodGroup 观察、缓存同步、事件回调
  apis/scheduling/v1alpha1/     PodGroup 的 Go 数据定义
  group/view.go                Pod 成员关联、索引和组视图
  group/state.go               组与 Pod 的内部状态定义
  workqueue/                   Unit、四个队列、Select / Policy、Transition 及测试
  staging/                     Center 接收、观察、评估及去重 worker；执行反馈待实现
config/crd/                    PodGroup CRD
upstream/                      上游源码阅读材料
```

Go 模块放在仓库根目录，源码放在 `src`，便于从根目录执行命令。

## 当前运行方式

需要 Go 1.24 或以上，以及可访问虚拟机集群的 kubeconfig。当前依赖固定为 client-go v0.34.7，真实集群版本还需要确认。

```sh
go run ./src/cmd/batchscheduler --kubeconfig /path/to/config
```

也支持 client-go 默认的 `KUBECONFIG` 和 `~/.kube/config` 加载规则。程序只读取全 namespace 的 Pod、Node 和本项目的 PodGroup，需要相应的 list/watch 权限；不会创建或绑定 Pod。连接地址、证书和凭据需适用于运行程序的机器。

连接集群前，需先由管理员安装本项目 CRD：

```sh
kubectl apply -f config/crd/scheduling.batchscheduler.io_podgroups.yaml
```

本次只准备了文件，未执行该命令。PodGroup API 为 `scheduling.batchscheduler.io/v1alpha1`，与上游同名 CR 分开。

启动后先等待三个 informer 的初始缓存同步，再通过 lister 打印对象数量，随后保持观察。按 Ctrl+C 停止；CRD 未安装、连接或权限有问题时，client-go 输出错误并重试，可以取消退出。对象数量只是观察验证，不能作为一致的调度资源快照。

## 本步阅读顺序

1. 入口如何创建 client 并调用 `informer.New`。
2. factory 如何创建 Pod/Node informer，为什么必须在启动前实例化。
3. `Start` 与 `WaitForCacheSync` 分别负责什么。
4. lister 为什么从缓存读取，为什么返回对象不能直接修改。

组关系的阅读入口见 [PodGroup 与 Pod 视图](docs/podgroup-view.md)。Select 已接通初始视图到工作队列的首次入队；持续业务事件处理器尚未注册。关联索引由 informer 的 Indexer 自动维护。informer 对象缓存与未来用于资源记账的调度缓存是不同职责。
