# BatchScheduler

[English](README.md) | [简体中文](README.zh-CN.md)

**A group-aware Kubernetes scheduler with elastic scheduling units and communication-aware placement.**

BatchScheduler schedules related Pods as a shared task. It assembles members into scheduling units, searches for a placement across the group, accounts for resources before binding, and coordinates execution feedback with Kubernetes observations.

The scheduler runs alongside the default Kubernetes scheduler. Workloads declare their group through a `PodGroup` custom resource and submit member Pods with matching ownership references.

## Features

- **Group admission.** `AllTogether` waits for the declared members and a complete placement before proceeding. `AllowPartial` lets available members run while others wait for another attempt.
- **Elastic scheduling units.** A Unit batches eligible members from one group. It can grow while waiting in Active; its member list is fixed when handed to the execution pipeline.
- **Communication-aware placement.** Explicit member relationships guide a bounded search over node, rack, zone, and cluster locality. Relationship priorities protect important links, while communication weights and Pod scores rank alternatives.
- **Shared resource accounting.** Filter, Score, and group planning share a cycle snapshot. Batch Assume checks the latest resource ledger, and Partial admission can retain the members that fit.
- **Asynchronous binding.** Serial planning and Assume feed a bounded Inflight channel consumed by configurable binding workers.
- **Feedback-driven retries.** A Buffer combines execution reports and informer observations, returns unplaced members to the queues, and schedules retries after relevant changes and backoff.
- **Placement history.** Successful attempts are recorded in BoltDB and exposed through the group-context interface.

## How It Works

```text
PodGroup + member Pods
        ↓
Informer / Group views → WorkQueue → Buffer
                                      ↓
                            Filter → Score → Group plan
                                      ↓
                                    Assume
                                      ↓
                             Inflight → Bind workers
                                      ↓
                         Report + informer observations
                                      ↓
                        Complete or return for retry
```

Informers maintain group views and update a shared ResourceCache. The Executor reads resource snapshots from that cache; Assume makes local reservations visible to subsequent attempts immediately. Binding results and observed API state return to the Buffer, which decides how each attempt proceeds.

| Component | Responsibility |
| --- | --- |
| Group views | Resolve PodGroup membership using namespace, owner UID, and Pod identity |
| WorkQueue | Assemble Units and manage Active, Fail, and Retry |
| Buffer | Track execution, evaluate group state, and apply decisions |
| Planner | Generate, compare, and select group placements |
| ResourceCache | Account for bound Pods and local assumptions |
| Executor / Inflight | Orchestrate scheduling and dispatch background binding |

## Submission Modes

| Mode | Behavior |
| --- | --- |
| `AllTogether` | The Buffer gathers the declared members. Planning and resource admission cover the whole attempt before binding starts. |
| `AllowPartial` | Eligible members can proceed together. Members left unplaced by filtering, planning, or Assume are returned for another attempt. |

Binding uses Kubernetes' per-Pod binding API. Scheduling completion refers to node assignment; application startup and completion remain observable through Pod status.

## Quick Start

Requirements: Go **1.24+**, `kubectl`, and a reachable Kubernetes cluster with a kubeconfig. Dependencies are pinned to Kubernetes **v1.34.7** and client-go **v0.34.7**.

```sh
git clone https://github.com/cheng-zhangpei/BatchScheduler.git
cd BatchScheduler

kubectl apply -f config/crd/scheduling.batchscheduler.io_podgroups.yaml
kubectl wait --for=condition=Established \
  crd/podgroups.scheduling.batchscheduler.io --timeout=60s

go run ./src/cmd/batchscheduler --kubeconfig /path/to/kubeconfig
```

The scheduler also accepts `KUBECONFIG` and the default kubeconfig discovery rules. Its credentials need read/watch access to Pods, Nodes, and PodGroups, and `create` access to `pods/binding` in workload namespaces.

In another terminal, create a group and its member Pods following the [user guide](docs/user-guide.md#submit-a-task). Set `schedulerName: batchscheduler` on members to keep them separate from the default scheduler. Membership is established through a controller owner reference to the actual PodGroup UID.

### PodGroup Example

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

Communication endpoints refer to each Pod's `scheduling.batchscheduler.io/member-id` label. Higher `priority` relationships are protected first; `weight` controls their contribution to communication scoring. See [communication placement](docs/user-guide.md#communication-placement) for the ranking rules and topology labels.

## Configuration

| Flag | Default | Description |
| --- | --- | --- |
| `--kubeconfig` | Default discovery | Kubernetes client configuration |
| `--max-inflight` | `4` | Maximum attempts in scheduling, waiting for binding, or binding |
| `--bind-workers` | `2` | Number of background binding workers |
| `--bind-timeout` | `30s` | Binding timeout for one attempt |
| `--history-db` | `.batchscheduler/history.db` | BoltDB file for successful placement records |

The binding worker count must be positive and no greater than the in-flight limit.

## Build and Test

```sh
go build -o batchscheduler ./src/cmd/batchscheduler
go test ./src/... -count=1
```

Tests use local fixtures and fake Kubernetes clients. They cover group assembly, planning, resource accounting, binding, and retry feedback without requiring a cluster.

For containers, build with the included Dockerfile. The [user guide](docs/user-guide.md#run-in-kubernetes) explains the deployment manifest, local minikube image loading, RBAC, and history storage.

## Repository Layout

```text
src/
  apis/scheduling/v1alpha1/  PodGroup API
  cmd/batchscheduler/       Entry point and component wiring
  group/                    Group views and member indexes
  informer/                 Kubernetes observation and resource updates
  workqueue/                Units, queues, admission, and retry transitions
  staging/                  Buffer, evaluation, error handling, and history
  scheduler/
    executor/               Scheduling orchestration and Inflight workers
    planner/                Group planning flow and plugin interfaces
    plugins/communication/  Communication-aware placement search
    resourcecache/          Resource accounting, Assume, and Forget
    steps/                  Filter, Score, Plan, Assume, Bind, and Recover
    upstream/               Native Kubernetes plugin adapters
config/                     CRD, workload example, and deployment manifest
docs/                       User guides and source walkthroughs
third_party/kubernetes/     Kubernetes source used by the adapters
```

## Documentation

- [User guide](docs/user-guide.md) · [中文使用指南](docs/user-guide.zh-CN.md)
- [Documentation index](docs/README.md)
- [WorkQueue and Buffer handoff](docs/queue-buffer-ownership.md)
- [Communication placement walkthrough](docs/communication-placement.md)
- [Binding pipeline](docs/binding.md)

## Contributing

Issues and focused pull requests are welcome. Describe the workload and expected behavior, keep component responsibilities explicit, and include tests for scheduling or state-transition changes.
