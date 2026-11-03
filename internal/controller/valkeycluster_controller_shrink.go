package controller

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cachev1alpha1 "github.com/halter/valkey-cluster-operator/api/v1alpha1"
	internalValkey "github.com/halter/valkey-cluster-operator/internal/controller/valkey"
)

// shardShrinkStep is a shrinking shard's next step: wait with a reason, fail a primary over to a staying replica, or shrink.
type shardShrinkStep struct {
	wait       string
	failoverTo *internalValkey.ClusterNode
	primaryPod string
}

// planShardShrink requires every pod from ordinal keep up to be a replica, since Parallel pod management removes them together.
func planShardShrink(stsName string, keep, current int32, clusterNodes []*internalValkey.ClusterNode) shardShrinkStep {
	nodeByPod := make(map[string]*internalValkey.ClusterNode, len(clusterNodes))
	for _, cn := range clusterNodes {
		nodeByPod[cn.Pod] = cn
	}
	stays := make(map[string]bool, keep)
	for ordinal := int32(0); ordinal < keep; ordinal++ {
		stays[fmt.Sprintf("%s-%d", stsName, ordinal)] = true
	}
	for ordinal := keep; ordinal < current; ordinal++ {
		pod := fmt.Sprintf("%s-%d", stsName, ordinal)
		node, ok := nodeByPod[pod]
		if !ok {
			return shardShrinkStep{wait: fmt.Sprintf("pod %s is not a Ready cluster node, so its role is unknown", pod)}
		}
		if !node.IsMaster() {
			continue
		}
		for _, cn := range clusterNodes {
			if cn.MasterNodeID == node.ID && stays[cn.Pod] {
				return shardShrinkStep{failoverTo: cn, primaryPod: pod}
			}
		}
		return shardShrinkStep{wait: fmt.Sprintf("primary %s has no replica among the pods that stay", pod)}
	}
	return shardShrinkStep{}
}

// shrinkShard takes a shard's StatefulSet down to statefulSetSize; remediateDeadNodes forgets the removed nodes.
func (r *ValkeyClusterReconciler) shrinkShard(ctx context.Context, valkeyCluster *cachev1alpha1.ValkeyCluster, sts *appsv1.StatefulSet) (*ctrl.Result, error) {
	logger := log.FromContext(ctx)
	keep := statefulSetSize(valkeyCluster)
	clusterNodes, err := r.buildClusterNodes(ctx, valkeyCluster)
	if err != nil {
		return &ctrl.Result{}, err
	}
	step := planShardShrink(sts.Name, keep, *sts.Spec.Replicas, clusterNodes)
	if step.wait != "" {
		logger.Info("Waiting to remove replicas from a shard", "StatefulSet", sts.Name, "reason", step.wait)
		return &ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	if step.failoverTo != nil {
		logger.Info("Failing over a primary that is to be removed to a replica that stays",
			"StatefulSet", sts.Name, "primaryPod", step.primaryPod, "replicaPod", step.failoverTo.Pod)
		replicaClient, err := r.NewValkeyClient(ctx, valkeyCluster, step.failoverTo.IP, VALKEY_PORT)
		if err != nil {
			return &ctrl.Result{}, fmt.Errorf("failed to create valkey client for failover on pod %s: %w", step.failoverTo.Pod, err)
		}
		defer replicaClient.Close()
		if err := replicaClient.Do(ctx, replicaClient.B().ClusterFailover().Build()).Error(); err != nil {
			return &ctrl.Result{}, fmt.Errorf("failed to fail %s over to %s: %w", step.primaryPod, step.failoverTo.Pod, err)
		}
		r.Recorder.Event(valkeyCluster, "Normal", "ReplicaScaleDownFailover",
			fmt.Sprintf("Primary %s is to be removed; failed over to %s, which stays", step.primaryPod, step.failoverTo.Pod))
		return &ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}
	current := *sts.Spec.Replicas
	if err := r.updateStatefulSet(ctx, valkeyCluster.Namespace, sts.Name, func(ss *appsv1.StatefulSet) *appsv1.StatefulSet {
		ss.Spec.Replicas = &keep
		return ss
	}); err != nil {
		return &ctrl.Result{}, err
	}
	r.Recorder.Event(valkeyCluster, "Normal", "ReplicaScaleDown",
		fmt.Sprintf("StatefulSet %s/%s shrinks from %d to %d pods; every removed pod is a replica", sts.Namespace, sts.Name, current, keep))
	return &ctrl.Result{Requeue: true}, nil
}
