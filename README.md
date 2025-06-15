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

What is **NOT** implemented:
- Valkey version upgrades. Right now only version 8.0.2 of Valkey is supported
- Services. The only way to connect to the Valkey cluster is via the pod IP.

## Prerequisites

In order to develop on `valkey-cluster-operator`, you'll want to install the following tools:

- [Kind](https://kind.sigs.k8s.io/docs/user/quick-start/#installation), used to spinning up a local Kubernetes cluster to install the operator into, as well as execute e2e tests against
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

Once done, you'll want to execute the following command to run the e2e test suite:

```console
$ dagger call e-2-e-test --sock /var/run/docker.sock
```