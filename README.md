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
