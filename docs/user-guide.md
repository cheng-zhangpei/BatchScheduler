# BatchScheduler User Guide

[English](user-guide.md) | [简体中文](user-guide.zh-CN.md) · [Project overview](../README.md)

This guide covers running the scheduler, submitting a task, configuring communication relationships, and observing scheduling results. Shell examples use Bash and can also be run in WSL.

## Install the PodGroup API

Use a kubeconfig that can reach your cluster, then install the CRD:

```sh
kubectl apply -f config/crd/scheduling.batchscheduler.io_podgroups.yaml
kubectl wait --for=condition=Established \
  crd/podgroups.scheduling.batchscheduler.io --timeout=60s
```

The API is `scheduling.batchscheduler.io/v1alpha1`, with a namespaced resource named `PodGroup`.

## Run Locally

Run the process from the repository root:

```sh
go run ./src/cmd/batchscheduler \
  --kubeconfig /path/to/kubeconfig \
  --max-inflight 4 \
  --bind-workers 2 \
  --bind-timeout 30s \
  --history-db .batchscheduler/history.db
```

The process waits for initial informer and resource-accounting synchronization before scheduling. It watches Pods, Nodes, and PodGroups, then processes group observations and queue notifications continuously.

The client identity needs `get`, `list`, and `watch` for the observed resources and `create` on `pods/binding` in task namespaces. `KUBECONFIG` and default kubeconfig discovery are also supported.

## Run in Kubernetes

The deployment manifest includes a namespace, service account, observation permissions, namespace-scoped binding permissions, and a single scheduler replica:

```sh
kubectl apply -f config/deploy/minikube-demo.yaml
kubectl -n batchscheduler-demo rollout status deployment/batchscheduler
kubectl -n batchscheduler-demo logs -f deployment/batchscheduler
```

The manifest pins `chengzhangpei/batchscheduler:20261103-preview`. To run the current checkout with minikube, build and load a local image, then update the deployment:

```sh
docker build -t batchscheduler:local .
minikube image load batchscheduler:local
kubectl -n batchscheduler-demo set image \
  deployment/batchscheduler scheduler=batchscheduler:local
kubectl -n batchscheduler-demo rollout status deployment/batchscheduler
```

For a remote cluster, push your build to an accessible registry and use that image reference instead. The demo grants binding access in `batchscheduler-demo`; grant the scheduler equivalent access when submitting tasks in another namespace.

The image stores history at `/var/lib/batchscheduler/history.db`. The demo mounts an `emptyDir` at this location; mount a persistent volume there to retain successful-placement history across Pod replacement.

## Submit a Task

A task consists of a PodGroup declaration and its member Pods. BatchScheduler accepts these objects directly; creating the group defines the task, and creating the Pods supplies its members.

First, create a namespace and a two-member group:

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

Read the group's UID and use it in each member's controller owner reference:

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

Use a container image available to your cluster. The scheduler assigns nodes; kubelet then starts the containers.

### Member Identity

Member Pods and their PodGroup share a namespace. The controller owner reference carries the group's API version, kind, name, and actual UID. This preserves the distinction between a group and a later group created with the same name.

Set `schedulerName: batchscheduler` to keep member Pods separate from the default scheduler. BatchScheduler identifies its workload through PodGroup ownership. The `member-id` label gives a stable name to a member referenced by communication rules; use a distinct value for each member.

### Choose a Submission Mode

| Setting | Task behavior |
| --- | --- |
| `members` | Expected member count; required and greater than zero |
| `submitMode: AllTogether` | Gather all declared members, find a complete placement, and admit the whole attempt before binding |
| `submitMode: AllowPartial` | Schedule eligible members together and return unplaced members for another attempt |

The default mode is `AllTogether`. Choose the mode when creating a task.

For Partial tasks, available members can proceed before the whole declared group arrives. If resources change after planning, Assume can admit the members that still fit. The others are recorded as Unplaced and sent through the retry path. Atomic admission keeps the whole resource batch together.

## Communication Placement

Add `spec.placement.communication` when the task has relationships whose placement matters. For the three-member example in [podgroup-communication.yaml](../config/examples/podgroup-communication.yaml), member labels are `worker-0`, `worker-1`, and `worker-2`.

Each relationship has these fields:

| Field | Meaning |
| --- | --- |
| `name` | Unique relationship name within the group |
| `from`, `to` | Endpoint `scheduling.batchscheduler.io/member-id` values |
| `preferredLevel` | Preferred locality: `Node`, `Rack`, `Zone`, or `Cluster` |
| `priority` | Higher values protect a relationship earlier when locality conflicts |
| `weight` | Positive contribution weight for communication scoring |

Declare relationships whose endpoints participate in the same scheduling attempt. `AllTogether` provides that grouping for the declared members.

### Topology Labels

| Level | Node comparison |
| --- | --- |
| `Node` | Same node name |
| `Rack` | Same nonempty `scheduling.batchscheduler.io/rack` label |
| `Zone` | Same nonempty `topology.kubernetes.io/zone` label |
| `Cluster` | Nodes outside the matching domains above |

For example, assign rack labels that reflect the cluster's physical layout:

```sh
kubectl label node node-a scheduling.batchscheduler.io/rack=rack-a --overwrite
kubectl label node node-b scheduling.batchscheduler.io/rack=rack-a --overwrite
```

Replace node names with those in your cluster. Locality is determined from node identity and labels.

### How Plans Are Ranked

The planner extends several candidate placements across the communication graph, checks cumulative resources and HostPorts, and retains a bounded set of alternatives. The default search retains up to four plans after each relationship or remaining member.

Relationships are ordered by descending priority, then name. Plans are compared by:

1. Each relationship's degradation depth, in that order; fewer levels of degradation wins at the first difference.
2. Weighted communication score, when degradation records match.
3. The sum of the selected nodes' Pod scores, when communication scores also match.

For locality ranks `Node=0`, `Rack=1`, `Zone=2`, `Cluster=3`:

```text
degradation = max(0, actual rank - preferred rank)
communication score = sum(weight * (3 - actual rank))
```

Closer alternatives remain eligible even when the preference is wider. Candidate retention also preserves different layout shapes, such as two members on one node or one member on each of two nodes. Resource accounting happens during trial placement and is committed by Assume after a plan is selected.

## Observe Results

```sh
kubectl -n batchscheduler-demo get podgroups.scheduling.batchscheduler.io
kubectl -n batchscheduler-demo get pods -o wide
kubectl -n batchscheduler-demo get pods -w
kubectl -n batchscheduler-demo describe pod demo-worker-0
```

A populated `NODE` column shows node assignment. `Running` and `Succeeded` describe the container lifecycle after scheduling. PodGroup contains the declaration; member Pod status shows the cluster's execution state.

Scheduler logs show cache synchronization, startup resource summaries, and errors encountered during observation or execution feedback. For a Kubernetes deployment:

```sh
kubectl -n batchscheduler-demo logs -f deployment/batchscheduler
```

A Partial task can show assigned members alongside Pending members. Retry waits for its relevant condition and backoff deadline. The Buffer keeps the executing members' state while the unplaced members form a new Unit.

For a Pending task, inspect the scheduler logs, the group's member count, controller owner references, resource requests, and node topology labels. These show whether the task is gathering members, waiting for capacity, or unable to satisfy its current placement.

## Build and Verify

```sh
go build -o batchscheduler ./src/cmd/batchscheduler
go test ./src/... -count=1
go test -race ./src/scheduler/resourcecache -count=1
```

Local tests cover whole-group admission, Partial placement, resource changes between planning and Assume, binding feedback, and retry after capacity is released. The [source documentation](README.md#source-walkthroughs) follows each component in more detail.

## Remove the Sample Task

```sh
kubectl -n batchscheduler-demo delete pod demo-worker-0 demo-worker-1
kubectl -n batchscheduler-demo delete podgroups.scheduling.batchscheduler.io demo-batch
```

The scheduler and namespace can remain running for the next task.
