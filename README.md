# BatchScheduler

A group-oriented Kubernetes scheduler built in Go, with explicit scheduling units, observable execution state, and condition-based retries.

BatchScheduler is being developed as a standalone scheduler inspired by the Kubernetes scheduling architecture. It explores how groups of related Pods can be admitted, tracked, and recovered together while supporting smaller scheduling units where partial progress is appropriate.

**Status: early development.** Resource observation, scheduling-unit queues, staged evaluation, and queue transitions are implemented. Active consumption connects to the Center, followed by native filtering, per-Pod scoring, communication-aware plan selection, local Assume/Forget, and Kubernetes binding requests. An explicit Inflight queue separates scheduling from binding workers. FastRetry consumption and recovery for partial or uncertain binding outcomes remain unfinished.

Communication relationships guide a bounded search over candidate placements: higher `priority` relationships are processed first, with names breaking ties. Different layouts remain available for later scoring. Resource consolidation scoring and selection across multiple scoring objectives are still pending.

## Project Goals

- **Coordinate related workloads.** Give related Pods a shared scheduling context and group-level progress tracking.
- **Support different submission modes.** Model whole-group and partial submission through a common scheduling-unit interface.
- **Make execution state explicit.** Track scheduling progress separately from the state observed in the Kubernetes API.
- **Recover through defined transitions.** Classify failures, confirm recovery, and retry when a relevant condition changes or a backoff expires.
- **Keep decisions separate from effects.** Evaluate state to produce decisions, then apply those decisions through dedicated queue and execution interfaces.
- **Maintain a readable implementation.** Keep observation, queue management, decision rules, and execution stages independently understandable and testable.

## Core Concepts

### PodGroup

`PodGroup` is a namespaced custom resource in `scheduling.batchscheduler.io/v1alpha1`. It declares the expected number of members and their submission mode.

```yaml
apiVersion: scheduling.batchscheduler.io/v1alpha1
kind: PodGroup
metadata:
  name: training-workers
  namespace: default
spec:
  members: 4
  submitMode: AllTogether
```

The supported modes are:

| Mode | Scheduling intent | Current unit assembly |
| --- | --- | --- |
| `AllTogether` | Admit the complete declared group as one scheduling unit | Wait for all expected members before initial admission |
| `AllowPartial` | Allow partial success and recovery without requiring the complete group | Batch currently eligible members into a unit without waiting for the whole group |

`AllTogether` currently defines admission and recovery scope. Transactional group binding is not implemented.

Member Pods are identified by a controller `ownerReference` pointing directly to the PodGroup, with the matching API version, kind, name, and UID. Pods must belong to the same namespace. Standalone Pods are outside the scheduling scope.

Creating a PodGroup does not create its Pods. A workload operator is planned to manage member creation and workload lifecycle. See [PodGroup and member views](docs/podgroup-view.md) for the association rules.

### Scheduling Unit

A scheduling unit represents the set of Pods handled in one attempt. A group can have multiple units, particularly in partial-submission mode.

Units can collect members during assembly. Membership is frozen before admission to a queue and remains fixed during execution and retry. A changed member set requires a new unit.

### Work Queues

| Queue | Purpose |
| --- | --- |
| `Active` | Units eligible for a scheduling attempt |
| `Fail` | Confirmed failures awaiting recovery confirmation or retry classification |
| `Retry` | Units waiting for a relevant event, a backoff deadline, or both |
| `FastRetry` | Reserved channel for narrowly admitted priority retries; admission policy is not implemented |

`Select` assembles units and performs initial admission. `Transition` manages movement between existing queues: `Fail → Retry` and `Retry → Active`.

Failures returned from execution require a separate handoff to the queue. An `Active → Fail` transition is not part of the queue migration model.

## Architecture

The implementation separates Kubernetes observations, scheduler decisions, and execution effects.

| Component | Responsibility |
| --- | --- |
| Informers | Watch Pods, Nodes, and PodGroups and maintain local caches |
| Group store | Associate PodGroups with member Pods and assemble query views |
| WorkQueue | Assemble units, track queue membership, and enforce retry transitions |
| Buffer / Center | Track units being processed and combine execution progress with cluster observations |
| Decision engine | Evaluate completion, failures, and normal execution progress |
| Error engine | Map supported Pod scheduling conditions to internal failures and dispatch error-specific handlers |
| Apply | Route decisions to selection, queue transitions, execution, or handoff functions |
| Executor | Run placement and Assume serially, then hand prepared attempts to Inflight |
| Inflight | Queue prepared attempts and run a configurable number of binding workers |
| Execution steps | Implement filtering, scoring, placement, reservation, binding API calls, and supported recovery |

The Center has two independent inputs:

- **`Accept`** receives a unit selected for execution and creates its attempt record.
- **`Observe`** refreshes cluster facts from the informer caches, including changes that arrive before a local unit exists.

`Report` provides a third input for results from scheduler-initiated operations. It identifies the operation being reported and advances the internal execution state.

Evaluation uses a separate queue of group keys. Repeated notifications for a group are coalesced, and a worker evaluates the group's current state. This notification queue is distinct from the queues that hold scheduling units.

`Evaluate` produces decisions without modifying execution records or moving queue entries. `Apply` checks and dispatches those decisions. Attempt records are removed only after the required execution and queue-side cleanup has been confirmed.

## Implementation Status

### Implemented

- Typed Pod and Node informers, a dynamic PodGroup informer, and initial cache synchronization.
- A shared resource ledger populated by Node and bound-Pod events, with startup waiting for initial accounting callbacks to finish. Filter reads its snapshot, and Pod scoring and group placement reuse that cycle's view. Batch Assume and execution-scoped Forget are connected to the execution path.
- UID-aware group membership and member indexes.
- Scheduling-unit assembly, frozen membership, and lifecycle tracking.
- Initial queue selection for whole-group and partial submission.
- Group filtering backed by six unmodified Kubernetes v1.34.7 filter plugins; unsupported hard constraints are explicitly rejected.
- Per-Pod scoring backed by four native plugins, with sorted candidate lists and submission mode forwarded to group planning.
- Active/FastRetry FIFO storage and keyed Fail/Retry storage.
- Center acceptance, observation, staged evaluation, action dispatch, and execution feedback.
- Executor-owned execution queue and step orchestration: placement and Assume run serially, then an explicit Inflight queue hands attempts to binding workers. Capacity, worker count, and binding timeout are configurable. Workers use client-go's Pod binding API and the existing Report interface.
- Successful binding reports, informer confirmation, history archival, and release of completed attempts. Confirmed rejection before any member binds can recover through Forget and Fail/Retry. Uncertain or partially successful binding keeps the attempt pending for future reconciliation.
- PodGroup placement declarations, frozen configuration passed through scheduling units, and a prototype that builds a limited set of communication-aware placements using cumulative resource and port checks.
- Mapping of `PodScheduled=False` conditions for `Unschedulable`, `SchedulerError`, and `SchedulingGated` into internal failure types.
- Version-checked queue transitions with retry classification and event/backoff conditions.
- BoltDB-backed successful-attempt history and group context shared across units; current bound peers remain distinct from historical placement.
- Release and failure-return handoff: completed attempts are removed, and failed members enter a new Fail-queue unit through Select before the Center drops the old record.
- Retry notifications from node changes, bound-Pod deletion, scheduling-gate removal, and periodic queue checks.
- Unit and fake-client integration tests for observation, decision generation, queue migration, and simulated execution.

### In Progress / Planned

- FastRetry consumption and admission rules.
- Review of search behavior and group placement semantics; resource consolidation scoring and selection across multiple scoring objectives.
- Recovery for submitted or uncertain bindings, and safe takeover of reservations from earlier executions.
- Recovery and handoff after partially successful group binding.
- Broader failure mapping, operation timeout reconciliation, and restart recovery.
- Buffer capacity controls, fairness, compaction, and FastRetry admission.
- A workload operator and validation against a live cluster.

The current tests establish behavior within their covered paths. They do not establish production readiness, live-cluster scheduling correctness, or performance characteristics.

## Getting Started

### Prerequisites

- Go **1.24 or newer**.
- A reachable Kubernetes API server and a valid kubeconfig.
- `kubectl` to install the PodGroup CRD.
- Permissions to list and watch Pods, Nodes, and PodGroups across the namespaces observed by the process.
- Permission to create the `pods/binding` subresource when using the execution path.

Kubernetes Go dependencies are pinned to **v0.34.7** in [go.mod](go.mod). A supported cluster-version matrix has not yet been established.

### Clone and Test

```sh
git clone https://github.com/cheng-zhangpei/BatchScheduler.git
cd BatchScheduler
go test ./src/... -count=1
```

The test suite uses in-memory fixtures and fake Kubernetes clients; it does not require a running cluster.

### Run the Current Prototype

Install the custom resource definition:

```sh
kubectl apply -f config/crd/scheduling.batchscheduler.io_podgroups.yaml
```

Start the process:

```sh
go run ./src/cmd/batchscheduler --kubeconfig /path/to/kubeconfig
```

The process also supports the standard `KUBECONFIG` environment variable and default kubeconfig discovery.

Binding settings are `--max-inflight` (default `4`, total scheduling/waiting/binding attempts), `--bind-workers` (default `2`), and `--bind-timeout` (default `30s` per attempt). Values must be positive, and the worker count must not exceed the in-flight limit.

At startup, it synchronizes the Pod, Node, and PodGroup caches, logs resource counts, and evaluates observed groups for initial admission. It continues processing observation and retry notifications until interrupted.

**The main process drains Active into the Center after cache synchronization.** The execution path filters, scores, selects a plan, reserves resources, and submits real Pod binding requests through Inflight workers. An API success means node assignment, not container readiness; resource reservations remain accounted for until informer updates confirm actual bindings. Before any member binds, confirmed rejection can recover through Forget and Fail/Retry. Partial success and uncertain outcomes remain pending; they are not reported as successful rollback. FastRetry consumption, earlier-execution takeover, failed-Report reconciliation, and selection using additional scoring objectives remain pending. An Active-to-Center admission error currently returns the unit to Active and stops the process, rather than discarding it or retrying it indefinitely. Automatic reassembly of stale queued units is pending. Successful scheduling snapshots are stored in a local BoltDB file (default `.batchscheduler/history.db`, configurable with `--history-db`). In-flight execution recovery and history retention policies remain pending. See [binding design and source walkthrough](docs/binding.md). Live-cluster validation has not been performed.

### Build

```sh
go build ./src/cmd/batchscheduler
```

## Repository Layout

```text
src/
  apis/scheduling/v1alpha1/   PodGroup API definitions
  cmd/batchscheduler/        Application entry point and component wiring
  group/                     Group views, member indexes, and internal states
  informer/                  Resource caches and change notifications
  scheduler/                 Shared filter and score contracts
    executor/                Execution queue, step orchestration, and reporting
    planner/                 Candidate plans, plugin contracts, and planning flow
    plugins/                 Concrete group planning plugins
    resourcecache/           Shared NodeInfo accounting and Pod assumptions
    steps/                   Filtering, placement, reservation, binding, and recovery
    upstream/                Native Kubernetes filter and score adapters
  staging/                   Shared messages and Center interface
    buffer/                  Attempt records, observation, dispatch, and feedback
    evaluation/              Pure decision stages
    history/                 BoltDB-backed successful-attempt records
    error_engine/            Observed-error mapping and failure handlers
  utils/                     Admission helpers
  workqueue/                 Scheduling units, queues, selection, and transitions
third_party/kubernetes/      Unmodified native filter source subset
config/crd/                  PodGroup custom resource definition
docs/                        Architecture and source-level documentation
progress.md                  Detailed implementation checkpoints
```

## Documentation

- [Buffer: source-level design and tradeoffs](docs/buffer.md) — records, evaluation stages, execution feedback, handoff, and known gaps.
- [Cross-unit context and successful records](docs/group-context.md) — persistence ordering, current versus historical placement, and remaining compaction work.
- [Pod scoring](docs/score.md) — native scoring adapters and the input passed to group planning.
- [Communication placement source walkthrough](docs/communication-placement.md) — candidate expansion, cumulative resources, scoring, and the next algorithm review.
- [First-version group placement design](docs/group-scoring-design.md) — communication-aware candidate plans, optional resource consolidation, and remaining final scoring work.
- [Communication-aware placement design](docs/communication-semantics.md) — relationship candidates, group plans, degradation rules, and scoring intent.
- [PodGroup and Pod views](docs/podgroup-view.md) — resource relationships and member lookup.
- [Architecture and implementation progress](progress.md) — current checkpoints and validation boundaries.
- [Project background and requirements](批任务组调度与失败恢复调度器-背景与需求说明书.md) — project scope and design background.

The detailed design documents and most source comments are currently written in Chinese. Historical checkpoints describe the implementation at the time they were recorded; the latest checkpoint takes precedence.

## Contributing

The project is evolving, and its APIs and execution contracts are subject to change. For architectural changes, discuss the intended behavior and scope in an issue before starting a large implementation.

Keep changes focused, explain state transitions and failure behavior, and include tests where behavior changes. Separate decision rules from side effects, and document any integration that remains incomplete.
