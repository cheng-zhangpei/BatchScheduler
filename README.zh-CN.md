# BatchScheduler

[English](README.md) | [简体中文](README.zh-CN.md)

**面向关联 Pod 的 Kubernetes 组调度器，支持弹性调度单元与通信感知放置。**

BatchScheduler 将一批相关 Pod 作为共同任务进行调度：组织成员、搜索组放置方案、在绑定前接纳资源，并结合执行反馈与集群观测推进任务。

调度器可以与 Kubernetes 默认调度器并行运行。任务通过 `PodGroup` 自定义资源声明组语义，通过成员 Pod 的所属关系提交调度负载。

## 核心功能

- **组接纳。** `AllTogether` 等待声明的成员到齐并找到完整放置方案后推进；`AllowPartial` 允许可接纳成员先执行，其余成员等待后续尝试。
- **弹性调度单元。** Unit 将同组可调度成员组织成批次。成员在 Active 中等待时可以继续加入，交给执行流程时固定名单。
- **通信感知放置。** 根据成员通信关系，在节点、机架、可用区和集群范围内搜索候选方案，通过链路优先级、通信权重和 Pod 基础分选择位置。
- **共享资源记账。** Filter、Score 和组规划复用本轮资源快照；Batch Assume 在最新资源账上复核，Partial 可以保留实际放得下的成员。
- **异步绑定。** 串行规划和 Assume 完成后，经有界 Inflight 通道交给可配置数量的后台绑定 Worker。
- **反馈驱动重试。** Buffer 汇合执行报告与 Informer 观测，将未放置成员回送队列，并结合相关事件和退避时间安排重试。
- **放置历史。** 使用 BoltDB 保存成功调度记录，通过组上下文接口查询。

## 工作流程

```text
PodGroup 与成员 Pod
        ↓
Informer / 组视图 → WorkQueue → Buffer
                                 ↓
                       Filter → Score → 组规划
                                 ↓
                               Assume
                                 ↓
                        Inflight → 绑定 Worker
                                 ↓
                         Report 与 Informer 观测
                                 ↓
                          完成本轮或回送重试
```

Informer 维护组视图，并更新共享 ResourceCache。Executor 从资源缓存获取快照，Assume 将本地暂占立即反映给后续调度。绑定反馈与 API 中的实际状态回到 Buffer，由它决定本轮任务如何继续。

| 组件 | 职责 |
| --- | --- |
| 组视图 | 通过 namespace、所属组 UID 和 Pod 身份关联成员 |
| WorkQueue | 组装 Unit，管理 Active、Fail 和 Retry |
| Buffer | 保存执行进度、评估组状态并应用决策 |
| Planner | 生成、比较并选择组放置方案 |
| ResourceCache | 维护已绑定 Pod 和本地暂占的资源占用 |
| Executor / Inflight | 编排调度步骤，分发后台绑定任务 |

## 提交模式

| 模式 | 行为 |
| --- | --- |
| `AllTogether` | Buffer 汇集声明的成员，整轮完成规划和资源接纳后开始绑定。 |
| `AllowPartial` | 当前可调度成员可以成批推进，Filter、规划或 Assume 中未放置的成员回送后续尝试。 |

绑定通过 Kubernetes 的逐 Pod Binding API 完成。调度完成表示已经分配节点，应用启动和运行结果通过 Pod 状态观测。

## 快速开始

准备 Go **1.24+**、`kubectl`，以及可访问的 Kubernetes 集群和 kubeconfig。项目依赖固定为 Kubernetes **v1.34.7**、client-go **v0.34.7**。

```sh
git clone https://github.com/cheng-zhangpei/BatchScheduler.git
cd BatchScheduler

kubectl apply -f config/crd/scheduling.batchscheduler.io_podgroups.yaml
kubectl wait --for=condition=Established \
  crd/podgroups.scheduling.batchscheduler.io --timeout=60s

go run ./src/cmd/batchscheduler --kubeconfig /path/to/kubeconfig
```

程序也支持 `KUBECONFIG` 环境变量和默认 kubeconfig 加载规则。运行账号需要读取、监听 Pod、Node、PodGroup，以及在任务所在 namespace 创建 `pods/binding` 的权限。

在另一个终端中，按照[使用指南](docs/user-guide.zh-CN.md#提交一个任务)创建组和成员 Pod。成员设置 `schedulerName: batchscheduler`，与默认调度器分工；成员所属关系通过指向真实 PodGroup UID 的 controller ownerReference 建立。

### PodGroup 示例

```yaml
apiVersion: scheduling.batchscheduler.io/v1alpha1
kind: PodGroup
metadata:
  name: training-workers
  namespace: batchscheduler-demo
spec:
  members: 3
  submitMode: AllTogether
  placement:
    communication:
      - name: worker-0-to-1
        from: worker-0
        to: worker-1
        preferredLevel: Node
        weight: 10
        priority: 100
      - name: worker-1-to-2
        from: worker-1
        to: worker-2
        preferredLevel: Rack
        weight: 5
        priority: 50
```

通信端点引用成员 Pod 的 `scheduling.batchscheduler.io/member-id` 标签值。`priority` 较高的关系优先受到保护，`weight` 决定其对通信评分的贡献。具体排序方式和拓扑标签见[通信放置配置](docs/user-guide.zh-CN.md#通信放置配置)。

## 运行参数

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `--kubeconfig` | 默认加载规则 | Kubernetes 客户端配置 |
| `--max-inflight` | `4` | 正在调度、等待绑定和正在绑定的最大执行轮数 |
| `--bind-workers` | `2` | 后台绑定 Worker 数量 |
| `--bind-timeout` | `30s` | 一轮绑定的超时时间 |
| `--history-db` | `.batchscheduler/history.db` | 成功放置记录的 BoltDB 文件 |

绑定 Worker 数量须为正，且不超过 Inflight 上限。

## 构建与测试

```sh
go build -o batchscheduler ./src/cmd/batchscheduler
go test ./src/... -count=1
```

测试使用本地数据和 fake Kubernetes 客户端，覆盖组装、规划、资源记账、绑定与重试反馈，无需启动集群。

容器镜像使用仓库中的 Dockerfile 构建。[使用指南](docs/user-guide.zh-CN.md#在-kubernetes-中运行)介绍部署清单、minikube 本地镜像加载、RBAC 和历史记录存储。

## 目录结构

```text
src/
  apis/scheduling/v1alpha1/  PodGroup API
  cmd/batchscheduler/       程序入口与组件接线
  group/                    组视图与成员索引
  informer/                 集群观测与资源更新
  workqueue/                Unit、队列、准入与重试流转
  staging/                  Buffer、评估、错误处理与历史记录
  scheduler/
    executor/               执行编排与 Inflight Worker
    planner/                组规划流程与插件接口
    plugins/communication/  通信感知放置搜索
    resourcecache/          资源记账、Assume 与 Forget
    steps/                  过滤、评分、规划、暂占、绑定与恢复
    upstream/               Kubernetes 原生插件适配
config/                     CRD、任务示例与部署清单
docs/                       使用指南与源码说明
third_party/kubernetes/     适配器使用的 Kubernetes 源码
```

## 文档

- [中文使用指南](docs/user-guide.zh-CN.md) · [User guide](docs/user-guide.md)
- [文档索引](docs/README.md)
- [WorkQueue 与 Buffer 的交接](docs/queue-buffer-ownership.md)
- [通信放置源码说明](docs/communication-placement.md)
- [绑定执行线](docs/binding.md)

## 参与贡献

欢迎提交 Issue 和聚焦的 Pull Request。请说明负载场景与预期行为，保持组件职责清晰，并为调度行为或状态流转的变更补充相应测试。
