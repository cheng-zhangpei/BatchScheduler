# BatchScheduler 使用指南

[English](user-guide.md) | [简体中文](user-guide.zh-CN.md) · [项目介绍](../README.zh-CN.md)

本文介绍运行调度器、提交任务、配置通信关系和查看调度结果的方法。Shell 示例使用 Bash，也可在 WSL 中执行。

## 安装 PodGroup API

使用能够访问集群的 kubeconfig，安装 CRD：

```sh
kubectl apply -f config/crd/scheduling.batchscheduler.io_podgroups.yaml
kubectl wait --for=condition=Established \
  crd/podgroups.scheduling.batchscheduler.io --timeout=60s
```

资源 API 为 `scheduling.batchscheduler.io/v1alpha1`，资源类型为 namespace 级的 `PodGroup`。

## 本地运行

在仓库根目录启动程序：

```sh
go run ./src/cmd/batchscheduler \
  --kubeconfig /path/to/kubeconfig \
  --max-inflight 4 \
  --bind-workers 2 \
  --bind-timeout 30s \
  --history-db .batchscheduler/history.db
```

程序会等待 Informer 和资源记账的初始同步完成，再启动调度。运行期间持续观测 Pod、Node、PodGroup，处理组状态变化与队列通知。

客户端身份需要对所观测资源的 `get`、`list`、`watch` 权限，以及任务 namespace 内 `pods/binding` 的 `create` 权限。程序也支持 `KUBECONFIG` 和默认 kubeconfig 加载规则。

## 在 Kubernetes 中运行

部署清单包含 namespace、ServiceAccount、资源观测权限、namespace 级绑定权限，以及单副本调度器：

```sh
kubectl apply -f config/deploy/minikube-demo.yaml
kubectl -n batchscheduler-demo rollout status deployment/batchscheduler
kubectl -n batchscheduler-demo logs -f deployment/batchscheduler
```

清单固定使用 `chengzhangpei/batchscheduler:20261103-preview` 镜像。在 minikube 中运行当前源码版本，可以构建、加载本地镜像，再更新 Deployment：

```sh
docker build -t batchscheduler:local .
minikube image load batchscheduler:local
kubectl -n batchscheduler-demo set image \
  deployment/batchscheduler scheduler=batchscheduler:local
kubectl -n batchscheduler-demo rollout status deployment/batchscheduler
```

远程集群使用可访问的镜像仓库推送构建结果，再替换 Deployment 的镜像地址。示例部署的绑定权限位于 `batchscheduler-demo`；使用其他 namespace 时，为调度器授予该 namespace 的对应权限。

镜像将历史记录写入 `/var/lib/batchscheduler/history.db`。示例在该路径挂载 `emptyDir`；需要在 Pod 替换后保留成功放置历史时，可在这里挂载持久卷。

## 提交一个任务

一个任务由 PodGroup 声明和成员 Pod 组成。BatchScheduler 可以直接接收这些对象：创建组定义任务，创建 Pod 提供成员。

先创建 namespace 和一个包含两个成员的组：

```sh
kubectl apply -f - <<'YAML'
apiVersion: v1
kind: Namespace
metadata:
  name: batchscheduler-demo
---
apiVersion: scheduling.batchscheduler.io/v1alpha1
kind: PodGroup
metadata:
  name: demo-batch
  namespace: batchscheduler-demo
spec:
  members: 2
  submitMode: AllTogether
YAML
```

读取组 UID，在每个成员中填写对应的 controller ownerReference：

```sh
GROUP_UID=$(kubectl -n batchscheduler-demo get podgroups.scheduling.batchscheduler.io demo-batch \
  -o jsonpath='{.metadata.uid}')

for i in 0 1; do
  kubectl apply -f - <<YAML
apiVersion: v1
kind: Pod
metadata:
  name: demo-worker-${i}
  namespace: batchscheduler-demo
  labels:
    scheduling.batchscheduler.io/member-id: worker-${i}
  ownerReferences:
    - apiVersion: scheduling.batchscheduler.io/v1alpha1
      kind: PodGroup
      name: demo-batch
      uid: "${GROUP_UID}"
      controller: true
spec:
  schedulerName: batchscheduler
  restartPolicy: Never
  containers:
    - name: worker
      image: busybox:1.36
      command: ["sh", "-c", "sleep 300"]
      resources:
        requests:
          cpu: 250m
          memory: 64Mi
YAML
done
```

容器镜像使用集群可获取的地址。调度器分配节点后，由 kubelet 启动容器。

### 成员身份

成员 Pod 与 PodGroup 处于同一 namespace。controller ownerReference 中保存组的 API 版本、类型、名称和真实 UID，因此可以区分同名重建前后的组。

成员设置 `schedulerName: batchscheduler`，与默认调度器分工。BatchScheduler 根据 PodGroup 所属关系识别调度负载。`member-id` 标签为通信规则提供稳定成员名称，每个成员使用不同的值。

### 选择提交模式

| 配置 | 任务行为 |
| --- | --- |
| `members` | 期望成员总数，必填且大于零 |
| `submitMode: AllTogether` | 汇集声明的成员，整轮完成放置和资源接纳后绑定 |
| `submitMode: AllowPartial` | 当前可调度成员成批执行，未放置成员进入后续尝试 |

默认模式为 `AllTogether`。创建任务时选择其提交模式。

Partial 任务可以在声明的全部成员到齐之前推进当前批次。若规划后资源变化，Assume 可以接纳仍放得下的成员，其余成员记入 Unplaced，经重试路径回送。Atomic 的资源接纳则保持整批一致。

## 通信放置配置

任务中的通信关系影响放置时，在 `spec.placement.communication` 中声明。仓库的 [podgroup-communication.yaml](../config/examples/podgroup-communication.yaml) 使用三个成员，标签值分别为 `worker-0`、`worker-1`、`worker-2`。

每条关系包含：

| 字段 | 含义 |
| --- | --- |
| `name` | 组内唯一的通信关系名称 |
| `from`、`to` | 两端成员的 `scheduling.batchscheduler.io/member-id` 标签值 |
| `preferredLevel` | 期望靠近等级：`Node`、`Rack`、`Zone`、`Cluster` |
| `priority` | 值越高，发生局部性冲突时越优先保护这条关系 |
| `weight` | 通信评分的正整数贡献权重 |

通信端点应参与同一次调度尝试。`AllTogether` 会将声明的成员汇集到同一轮中。

### 拓扑标签

| 等级 | 节点比较方式 |
| --- | --- |
| `Node` | 节点名相同 |
| `Rack` | 非空的 `scheduling.batchscheduler.io/rack` 标签相同 |
| `Zone` | 非空的 `topology.kubernetes.io/zone` 标签相同 |
| `Cluster` | 不属于前面匹配的拓扑域 |

例如，按实际物理布局为节点设置机架标签：

```sh
kubectl label node node-a scheduling.batchscheduler.io/rack=rack-a --overwrite
kubectl label node node-b scheduling.batchscheduler.io/rack=rack-a --overwrite
```

节点名替换为集群中的实际名称。靠近程度通过节点身份和拓扑标签判断。

### 方案比较方式

Planner 沿通信图扩展多套候选放置，累计检查资源和 HostPort，并保留有限数量的备选。每条关系或剩余成员处理后，默认最多保留四套方案。

关系先按优先级降序排列，同优先级按名称排列。方案依次比较：

1. 按上述关系顺序比较降级步数，第一处不同以降级更少者优先。
2. 降级记录一致时，比较加权通信分。
3. 通信分也一致时，比较所选节点的 Pod 基础分之和。

拓扑编号为 `Node=0`、`Rack=1`、`Zone=2`、`Cluster=3`：

```text
降级步数 = max(0, 实际等级编号 - 期望等级编号)
通信分 = sum(链路 weight × (3 - 实际等级编号))
```

即使声明了较宽的期望范围，更靠近的放置仍可成为候选。剪枝也会为不同分布形状保留代表，例如两成员同节点、两成员分别占一个节点。搜索阶段在资源副本上试放，选定方案后由 Assume 提交资源占用。

## 查看调度结果

```sh
kubectl -n batchscheduler-demo get podgroups.scheduling.batchscheduler.io
kubectl -n batchscheduler-demo get pods -o wide
kubectl -n batchscheduler-demo get pods -w
kubectl -n batchscheduler-demo describe pod demo-worker-0
```

`NODE` 列显示节点分配，`Running`、`Succeeded` 表示调度后的容器运行状态。PodGroup 保存任务声明，成员 Pod 的状态展示集群中的实际执行情况。

调度器日志展示缓存同步、启动资源信息，以及观测或执行反馈过程中出现的错误。集群部署可通过以下命令查看：

```sh
kubectl -n batchscheduler-demo logs -f deployment/batchscheduler
```

Partial 任务可能同时存在已分配节点的成员和 Pending 成员。重试会等待相关条件和退避时间，Buffer 继续保存执行成员的状态，未放置成员则形成新的 Unit。

任务持续 Pending 时，可以结合调度器日志，检查成员数量、controller ownerReference、资源申请和节点拓扑标签，确认任务正在等待成员、等待容量，还是当前放置条件无法满足。

## 构建与验证

```sh
go build -o batchscheduler ./src/cmd/batchscheduler
go test ./src/... -count=1
go test -race ./src/scheduler/resourcecache -count=1
```

本地测试覆盖整组接纳、Partial 放置、规划后到 Assume 之间的资源变化、绑定反馈，以及容量释放后的重试。各组件的详细阅读入口见[源码文档](README.md#source-walkthroughs)。

## 移除示例任务

```sh
kubectl -n batchscheduler-demo delete pod demo-worker-0 demo-worker-1
kubectl -n batchscheduler-demo delete podgroups.scheduling.batchscheduler.io demo-batch
```

调度器和 namespace 可以继续保留，供下一个任务使用。
