# valkey-cluster-operator

A Kubernetes Operator to deploy and manage Valkey Clusters.

The main goal of this Kubernetes operator is to provide a simple custom
resource definition that can be used to deploy a Valkey cluster in a Kubernetes
environment.

The operator provides the following features:

- Scaling up and down of CPU, memory and storage sizes.
- Scaling up and down the number of replicas per shard in the cluster
- Resharding up and down, you can change the number of shards in a cluster and
the Operator will handle resharding the slots for you.
- Automatic disk scaling: disk usage is monitored and volumes grow on their own,
so users don't need to think about disk sizes at all.
- Declarative Valkey config management: `spec.valkeyConfig.parameters` are
live-applied to running pods (no restart needed for runtime-settable
directives), runtime drift converges back to spec, and directives that cannot
be set at runtime are applied through a health-gated rolling restart.

What is **NOT** implemented:

- Services. The only way to connect to the Valkey cluster is via the pod IP.

## Disk auto-scaling

You don't need to set a disk size. The operator provisions each node's data
volume at 1Gi (or at `spec.storage.resources.requests.storage` if set, which
acts as the initial/minimum size) and monitors disk usage of every pod by
running `df` against the data mount every five minutes. When the fullest volume in
the cluster goes above 50% used, the operator grows the target size for **all**
volumes in the cluster by 50%, rounded up to a whole Gi. The current target
size is tracked in `status.storageSize`; volumes only ever grow, never shrink.

Optionally, `spec.storageLimit` caps the growth:

```yaml
spec:
  storageLimit: 100Gi
```

When the limit is reached, the operator stops expanding, emits a warning event
and sets the `StorageLimited` status condition; the condition clears once usage
drops back below the threshold or the limit is raised.

Notes:

- Volume expansion requires a StorageClass with `allowVolumeExpansion: true`
  (for example AWS EBS gp3). On storage classes without it (such as kind's
  local-path provisioner) the operator sets the `StorageLimited` condition once
  and disables auto-scaling for the cluster, including the usage measurements.
- AWS EBS allows one modification per volume per ~6 hours; the operator waits
  for an expansion to complete before requesting another one.

## Valkey configuration management

`valkey-server` reads its config file once at process start, and the config
file is mounted from a ConfigMap via `subPath`, so running pods never see file
updates. The operator therefore manages config in both directions:

- **Live apply.** Every reconcile, each directive in
  `spec.valkeyConfig.parameters` is checked against every running pod
  (`CONFIG GET`, compared semantically — memory values and
  `client-output-buffer-limit` classes are normalised) and applied with
  `CONFIG SET` when it differs. Declarative changes reach running pods without
  restarts, and manual `CONFIG SET` drift (e.g. incident remediation) converges
  back to spec instead of silently reverting on the next pod restart.
- **Health-gated restart fallback.** A directive the server rejects with
  "can't set immutable config" (or "can't set protected config") can only be
  applied by a restart onto the updated config file. Such pods are restarted
  one at a time, gated on full cluster health including replica catch-up (the
  same discipline as image rolling updates), replicas before primaries.
- **Rejected values do not restart pods.** A directive the server rejects for
  its *value* (e.g. `repl-backlog-size banana`) is surfaced as a warning event
  and skipped: the same value sits in the config file, and valkey refuses to
  boot on an invalid file directive, so restarting would trade a running pod
  for a crash-looping one. Directives the server does not recognise at all are
  likewise skipped with a warning.
- **`rawConfig` changes still require a restart.** The live-apply path manages
  only name/value `parameters` (and the operator defaults); an edit to
  `spec.valkeyConfig.rawConfig` updates the ConfigMap but takes effect only as
  pods restart.

### Operator-managed defaults

For clusters **not** using `spec.valkeyConfig.rawConfig` (raw config is expert
mode — the operator does not layer defaults it cannot see overridden) and whose
image tag positively parses as Valkey >= 8.0, the operator applies
replication-tuning defaults, derived from the pod memory limit:

| Directive | Default | Rationale |
|---|---|---|
| `dual-channel-replication-enabled` | `yes` | Moves full-sync buffering off the primary, structurally avoiding the primary-side output-buffer overrun that kills full syncs of large shards. |
| `repl-backlog-size` | `clamp(memory/16, 10MiB, 512MiB)` | The compiled-in 10MiB is seconds of backlog on a busy shard; too small a backlog turns every transient disconnect into a full resync. |
| `client-output-buffer-limit` | `replica clamp(memory/2, 64MiB, 4GiB) <hard/2> 120` | The compiled-in 256MiB hard limit kills full syncs of multi-GiB shards; conversely 256MiB exceeds the whole pod on small caches. |

`spec.valkeyConfig.parameters` entries are applied after the defaults, so a
per-cluster value always overrides the fleet default. The defaults were sized
from the SP-1527 incident (a 9.4GB shard at ~5.4MB/s of replication write
traffic stuck in a full-resync crash loop for ~6 hours under the compiled-in
defaults).

## Prerequisites

In order to develop on `valkey-cluster-operator`, you'll want to install the following tools:

- [Kind](https://kind.sigs.k8s.io/docs/user/quick-start/#installation), used for spinning up a local Kubernetes cluster. This is use to install the operator, as well as to execute e2e tests against the running operator (and the resources it manages)
- [Dagger](https://docs.dagger.io/install), a task runner that handles most of what `Makefile` offers, with enhanced caching and use of containers for reproducibility.

## Getting Started

`valkey-cluster-operator` is built using [Kube Builder](https://book.kubebuilder.io/introduction) which handles a lot of boilerplate.

For example, some changes you want to make will likely be to the `ValkeyCluster` [Custom Resource Definition (CRD)](https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/). Those changes are made to `api/v1alpha1/valkeycluster_types.go` and then Kube Builder handles regenerating the CRD definition found at `config/crd/bases/cache.halter.io_valkeyclusters.yaml`.

Once your changes are made, you'll want to run the e2e tests which we'll need Dagger for.

All of the functions available via Dagger can be seen like so:

```console
$ dagger functions
▶ connect 0.2s
▶ load module: . 0.5s

Name                           Description
build                          Build the application container
build-and-load-locally         -
build-manager                  Build the application binary
[...]
```

First, you'll want to build the Go binary for the operator, package it up into a Docker image and load that Docker image into Kind. That sounds a lot but only requires the following step:

```console
$ dagger call build-and-load-locally --sock /var/run/docker.sock
▶ connect 0.2s
▶ load module: . 0.3s
● parsing command line arguments 0.0s

● Host.unixSocket(path: "/var/run/docker.sock"): Socket! 0.0s

● valkeyClusterOperator: ValkeyClusterOperator! 0.0s
▶ .buildAndLoadLocally(
  ┆ source: Host.directory(path: "/Users/cooluser/halter/open-source/platform/valkey-cluster-operator", exclude: [], noCache: true): Directory!
  ┆ sock: Host.unixSocket(path: "/var/run/docker.sock"): Socket!
  ): Void 32.6s
```

> [!WARNING]
> Make sure that you don't run `make test-e2e` instead as it will generate binaries specific to your platform (in `bin`) and will also load the built image into Docker, instead of into your kind cluster.

Once done, you'll want to execute the following command to run the e2e test suite:

```console
dagger call e-2-e-test --sock /var/run/docker.sock
```
