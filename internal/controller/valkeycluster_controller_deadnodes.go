package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cachev1alpha1 "github.com/halter/valkey-cluster-operator/api/v1alpha1"
	internalValkey "github.com/halter/valkey-cluster-operator/internal/controller/valkey"
)

// remediateDeadNodes recovers from fail-flagged cluster nodes that no pod
// backs anymore: promote a surviving replica, otherwise cover the orphaned
// slots via cluster fix, then CLUSTER FORGET the dead ID on every live node.
func (r *ValkeyClusterReconciler) remediateDeadNodes(ctx context.Context, valkeyCluster *cachev1alpha1.ValkeyCluster, clusterNodes []*internalValkey.ClusterNode) (*ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if len(clusterNodes) == 0 {
		return nil, nil
	}

	podList := &corev1.PodList{}
	listOpts := []client.ListOption{
		client.InNamespace(valkeyCluster.Namespace),
		client.MatchingLabels(labelsForValkeyCluster(valkeyCluster.Name)),
	}
	if err := r.List(ctx, podList, listOpts...); err != nil {
		return nil, err
	}
	representedPods := make(map[string]bool, len(clusterNodes))
	liveNodeIDs := make(map[string]bool, len(clusterNodes))
	for _, cn := range clusterNodes {
		representedPods[cn.Pod] = true
		liveNodeIDs[cn.ID] = true
	}
	for _, pod := range podList.Items {
		// A restarting pod keeps its node ID via its PVC — only act once
		// every pod is accounted for.
		if pod.DeletionTimestamp != nil || !representedPods[pod.Name] {
			logger.Info("Skipping dead-node detection: pod not represented in cluster nodes",
				"pod", pod.Name)
			return nil, nil
		}
	}

	valkeyClient, err := r.NewValkeyClient(ctx, valkeyCluster, clusterNodes[0].IP, VALKEY_PORT)
	if err != nil {
		return nil, fmt.Errorf("failed to create valkey client for dead-node detection: %w", err)
	}
	defer valkeyClient.Close()
	topologyTxt, err := valkeyClient.Do(ctx, valkeyClient.B().ClusterNodes().Build()).ToString()
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster topology for dead-node detection: %w", err)
	}
	topology, err := internalValkey.ParseClusterNodes(topologyTxt)
	if err != nil {
		return nil, fmt.Errorf("failed to parse cluster topology for dead-node detection: %w", err)
	}

	deadNodes := internalValkey.FindDeadNodes(topology, liveNodeIDs)
	if len(deadNodes) == 0 {
		return nil, nil
	}

	for _, dead := range deadNodes {
		logger.Info("Detected dead cluster node with no backing pod",
			"nodeID", dead.ID,
			"slotCount", dead.SlotCount(),
			"flags", dead.Flags)
	}

	for _, dead := range deadNodes {
		if !dead.HasSlots() {
			continue
		}
		for _, cn := range clusterNodes {
			if cn.MasterNodeID != dead.ID {
				continue
			}
			logger.Info("Promoting surviving replica of dead node via CLUSTER FAILOVER TAKEOVER",
				"replicaPod", cn.Pod,
				"replicaID", cn.ID,
				"deadNodeID", dead.ID)
			replicaClient, err := r.NewValkeyClient(ctx, valkeyCluster, cn.IP, VALKEY_PORT)
			if err != nil {
				return nil, fmt.Errorf("failed to create valkey client for replica takeover on pod %s: %w", cn.Pod, err)
			}
			defer replicaClient.Close()
			if err := replicaClient.Do(ctx, replicaClient.B().ClusterFailover().Takeover().Build()).Error(); err != nil {
				return nil, fmt.Errorf("failed to promote replica %s over dead node %s: %w", cn.Pod, dead.ID, err)
			}
			r.Recorder.Event(valkeyCluster, "Warning", "DeadNodeTakeover",
				fmt.Sprintf("Node %s owns %d slots but has no backing pod; promoted surviving replica %s via CLUSTER FAILOVER TAKEOVER", dead.ID, dead.SlotCount(), cn.Pod))
			return &ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
	}

	needsSlotTakeover := false
	for _, dead := range deadNodes {
		if dead.HasSlots() {
			needsSlotTakeover = true
		}
	}
	if needsSlotTakeover {
		jobMgr := r.NewValkeyJobManager()
		if err := jobMgr.FixClusterSlots(ctx, valkeyCluster, logger); err != nil {
			if errors.Is(err, errValkeyCliJobStillRunning) {
				logger.Info("valkey-cli Job still running, requeueing before dead-node slot takeover",
					"requeueAfter", "30s")
				return &ctrl.Result{RequeueAfter: 30 * time.Second}, nil
			}
			return nil, fmt.Errorf("failed to take over slots of dead nodes: %w", err)
		}
		r.Recorder.Event(valkeyCluster, "Warning", "DeadNodeSlotTakeover",
			fmt.Sprintf("Reassigned slots owned by %d dead node(s) with no surviving replica to reachable primaries", len(deadNodes)))
	}

	for _, dead := range deadNodes {
		forgotten := 0
		for _, cn := range clusterNodes {
			nodeClient, err := r.NewValkeyClient(ctx, valkeyCluster, cn.IP, VALKEY_PORT)
			if err != nil {
				logger.Info("Could not create valkey client to forget dead node, will retry next reconcile",
					"pod", cn.Pod, "deadNodeID", dead.ID, "error", err.Error())
				continue
			}
			defer nodeClient.Close()
			if err := nodeClient.Do(ctx, nodeClient.B().ClusterForget().NodeId(dead.ID).Build()).Error(); err != nil {
				logger.Info("CLUSTER FORGET rejected, will retry next reconcile",
					"pod", cn.Pod, "deadNodeID", dead.ID, "error", err.Error())
				continue
			}
			forgotten++
		}
		logger.Info("Forgot dead cluster node",
			"deadNodeID", dead.ID,
			"forgottenOn", forgotten,
			"liveNodes", len(clusterNodes))
		r.Recorder.Event(valkeyCluster, "Normal", "DeadNodeForgotten",
			fmt.Sprintf("Removed dead node %s (no backing pod) from %d of %d live nodes via CLUSTER FORGET", dead.ID, forgotten, len(clusterNodes)))
	}

	return &ctrl.Result{RequeueAfter: 15 * time.Second}, nil
}
