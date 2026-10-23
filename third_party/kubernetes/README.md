# Kubernetes scheduler source subset

Pinned upstream: **Kubernetes v1.34.7** (`k8s.io/kubernetes`), matching this project's v0.34.7 Kubernetes libraries.

This directory contains the unmodified non-test Go files from these six plugin packages and their transitive dependencies inside `k8s.io/kubernetes`:

- `NodeName`
- `NodeUnschedulable`
- `TaintToleration`
- `NodeAffinity`
- `NodePorts`
- `NodeResourcesFit`

The subset contains 21 packages and 54 Go files, approximately 1 MB. Shared framework types, configuration, resource helpers, metrics and feature definitions account for much of this size. Dependency code being present does not mean its features are enabled or implemented by BatchScheduler.

`SOURCE.json` records the source version, root packages, and SHA-256 of every copied Go file. Files available in the local `upstream/kubernetes` sparse checkout were copied from there after comparison with the published v1.34.7 module. Missing packages came from that same published module in the Go module cache.

The original source files and copyright headers are unchanged. `LICENSE` is the upstream Apache 2.0 license. The local `go.mod` is deliberately limited to dependencies imported by this subset, using published v0.34.7 versions for Kubernetes staging modules. The root module uses `replace k8s.io/kubernetes => ./third_party/kubernetes`.

BatchScheduler's adapters live in `src/scheduler/upstream/`. The filter adapter initializes six plugins, prepares `NodeInfo` and per-Pod `CycleState`, calls `PreFilter`/`Filter`, and translates the returned status. The score adapter reuses the same node view and calls `PreScore`/`Score`/`NormalizeScore` for NodeResourcesFit, NodeResourcesBalancedAllocation, NodeAffinity and TaintToleration. All four score plugins currently have weight 1; resource scoring uses equally weighted CPU and memory. These scoring implementations were already present in this source subset. The adapters do not initialize the full Kubernetes scheduler or run binding/event callbacks.

This is **not the complete default Kubernetes filter profile**. Inter-Pod affinity, topology spread, storage and DRA plugins remain unconnected. The adapter explicitly rejects incoming Pods with unsupported hard constraints instead of silently ignoring them. It also rejects Pods with outstanding scheduling gates. Soft preferences are not evaluated in this filtering phase.

Node data is copied when an adapter is constructed. It must include all assigned Pods, including workloads managed by other schedulers. Assume/Forget accounting and group trial placements are not implemented here; callers will need to supply an updated, deduplicated resource view for those operations.

When upgrading, regenerate the dependency subset from a single upstream version, keep the manifest and license, align the staging-module versions, and run the adapter tests before using it. Do not edit the copied plugin files to add group behavior; implement group planning in `src/scheduler`.
