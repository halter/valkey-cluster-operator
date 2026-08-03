package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cachev1alpha1 "github.com/halter/valkey-cluster-operator/api/v1alpha1"
	internalValkey "github.com/halter/valkey-cluster-operator/internal/controller/valkey"
)

// remediateDeadNodes recovers from fail-flagged cluster nodes that no pod
// backs anymore, one idempotent step per reconcile so any partial state
// converges: promote a surviving replica of a dead node, CLUSTER FORGET the
// dead ID on every live node, then cover slots left assigned to nobody via
// CLUSTER ADDSLOTSRANGE on a live primary. valkey-cli's fix is unusable
// here: it prompts for slot coverage even under --cluster-yes.
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

	// A running valkey-cli Job (e.g. a reshard) may be moving slots; acting
	// on a topology snapshot taken mid-migration could misread coverage.
	runningJob, err := r.settleValkeyCliJobs(ctx, valkeyCluster, logger)
	if err != nil {
		return nil, err
	}
	if runningJob != nil {
		return nil, nil
	}

	views := make([][]*internalValkey.ClusterNode, 0, len(clusterNodes))
	deadNodes := make(map[string]*internalValkey.ClusterNode)
	for _, cn := range clusterNodes {
		valkeyClient, err := r.NewValkeyClient(ctx, valkeyCluster, cn.IP, VALKEY_PORT)
		if err != nil {
			return nil, fmt.Errorf("failed to create valkey client for dead-node detection on pod %s: %w", cn.Pod, err)
		}
		defer valkeyClient.Close()
		topologyTxt, err := valkeyClient.Do(ctx, valkeyClient.B().ClusterNodes().Build()).ToString()
		if err != nil {
			return nil, fmt.Errorf("failed to get cluster topology from pod %s: %w", cn.Pod, err)
		}
		view, err := internalValkey.ParseClusterNodes(topologyTxt)
		if err != nil {
			return nil, fmt.Errorf("failed to parse cluster topology from pod %s: %w", cn.Pod, err)
		}
		views = append(views, view)
		for _, dead := range internalValkey.FindDeadNodes(view, liveNodeIDs) {
			if existing, ok := deadNodes[dead.ID]; !ok || (!existing.HasSlots() && dead.HasSlots()) {
				deadNodes[dead.ID] = dead
			}
		}
	}

	// Promote a surviving replica of a dead node first: its data survives,
	// and no live node may still replicate a dead ID when FORGET runs.
	for deadID, dead := range deadNodes {
		for _, cn := range clusterNodes {
			if cn.MasterNodeID != deadID {
				continue
			}
			logger.Info("Promoting surviving replica of dead node via CLUSTER FAILOVER TAKEOVER",
				"replicaPod", cn.Pod,
				"replicaID", cn.ID,
				"deadNodeID", deadID)
			replicaClient, err := r.NewValkeyClient(ctx, valkeyCluster, cn.IP, VALKEY_PORT)
			if err != nil {
				return nil, fmt.Errorf("failed to create valkey client for replica takeover on pod %s: %w", cn.Pod, err)
			}
			defer replicaClient.Close()
			if err := replicaClient.Do(ctx, replicaClient.B().ClusterFailover().Takeover().Build()).Error(); err != nil {
				return nil, fmt.Errorf("failed to promote replica %s over dead node %s: %w", cn.Pod, deadID, err)
			}
			r.Recorder.Event(valkeyCluster, "Warning", "DeadNodeTakeover",
				fmt.Sprintf("Node %s owns %d slots but has no backing pod; promoted surviving replica %s via CLUSTER FAILOVER TAKEOVER", deadID, dead.SlotCount(), cn.Pod))
			return &ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
	}

	if len(deadNodes) > 0 {
		for deadID := range deadNodes {
			logger.Info("Detected dead cluster node with no backing pod",
				"nodeID", deadID,
				"slotCount", deadNodes[deadID].SlotCount(),
				"flags", deadNodes[deadID].Flags)
			forgotten := 0
			for _, cn := range clusterNodes {
				nodeClient, err := r.NewValkeyClient(ctx, valkeyCluster, cn.IP, VALKEY_PORT)
				if err != nil {
					logger.Info("Could not create valkey client to forget dead node, will retry next reconcile",
						"pod", cn.Pod, "deadNodeID", deadID, "error", err.Error())
					continue
				}
				defer nodeClient.Close()
				err = nodeClient.Do(ctx, nodeClient.B().ClusterForget().NodeId(deadID).Build()).Error()
				if err != nil && !strings.Contains(err.Error(), "Unknown node") {
					logger.Info("CLUSTER FORGET rejected, will retry next reconcile",
						"pod", cn.Pod, "deadNodeID", deadID, "error", err.Error())
					continue
				}
				forgotten++
			}
			r.Recorder.Event(valkeyCluster, "Normal", "DeadNodeForgotten",
				fmt.Sprintf("Removed dead node %s (no backing pod) from %d of %d live nodes via CLUSTER FORGET", deadID, forgotten, len(clusterNodes)))
		}
		// Coverage is re-derived from fresh views next reconcile, once every
		// node has dropped the dead entries.
		return &ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	uncovered := internalValkey.UncoveredSlotRanges(views)
	if len(uncovered) == 0 {
		return nil, nil
	}

	var target *internalValkey.ClusterNode
	for _, cn := range clusterNodes {
		if cn.IsMaster() {
			target = cn
			break
		}
	}
	if target == nil {
		return nil, fmt.Errorf("no live primary to assign %d uncovered slot ranges to", len(uncovered))
	}
	targetClient, err := r.NewValkeyClient(ctx, valkeyCluster, target.IP, VALKEY_PORT)
	if err != nil {
		return nil, fmt.Errorf("failed to create valkey client for slot coverage on pod %s: %w", target.Pod, err)
	}
	defer targetClient.Close()
	slotCount := 0
	for _, sr := range uncovered {
		logger.Info("Assigning uncovered slots to live primary",
			"pod", target.Pod,
			"start", sr.Start,
			"end", sr.End)
		err := targetClient.Do(ctx, targetClient.B().ClusterAddslotsrange().StartSlotEndSlot().StartSlotEndSlot(int64(sr.Start), int64(sr.End)).Build()).Error()
		if err != nil {
			return nil, fmt.Errorf("failed to assign uncovered slots %d-%d to pod %s: %w", sr.Start, sr.End, target.Pod, err)
		}
		slotCount += sr.End - sr.Start + 1
	}
	r.Recorder.Event(valkeyCluster, "Warning", "SlotCoverageRestored",
		fmt.Sprintf("Assigned %d slots owned by no live node to %s; data in those slots is lost", slotCount, target.Pod))
	return &ctrl.Result{RequeueAfter: 15 * time.Second}, nil
}
