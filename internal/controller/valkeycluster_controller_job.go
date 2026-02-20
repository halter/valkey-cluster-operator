package controller

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-logr/logr"
	cachev1alpha1 "github.com/halter/valkey-cluster-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// ValkeyJobManager provides high-level operations for managing valkey cluster operations via Kubernetes Jobs.
// Each method represents a specific cluster management task.
type ValkeyJobManager struct {
	reconciler *ValkeyClusterReconciler
}

// NewValkeyJobManager creates a new job manager
func (r *ValkeyClusterReconciler) NewValkeyJobManager() *ValkeyJobManager {
	return &ValkeyJobManager{
		reconciler: r,
	}
}

// getTargetPodAddress gets the IP address of the target pod for cluster operations.
// By default, it targets the first pod in the first shard ({cluster-name}-0-0).
func (m *ValkeyJobManager) getTargetPodAddress(ctx context.Context, valkeyCluster *cachev1alpha1.ValkeyCluster) (string, error) {
	logger := log.FromContext(ctx)

	// Target the first pod in the first shard
	podName := fmt.Sprintf("%s-0-0", valkeyCluster.Name)

	logger.Info("Looking up target pod for cluster operation",
		"podName", podName,
		"namespace", valkeyCluster.Namespace)

	// Get the pod to retrieve its IP
	pod := &corev1.Pod{}
	err := m.reconciler.Get(ctx, types.NamespacedName{
		Name:      podName,
		Namespace: valkeyCluster.Namespace,
	}, pod)
	if err != nil {
		logger.Error(err, "Failed to get target pod", "podName", podName)
		return "", fmt.Errorf("Failed to get target pod %s: %w", podName, err)
	}

	logger.Info("Found target pod",
		"podName", podName,
		"podIP", pod.Status.PodIP,
		"podPhase", pod.Status.Phase)

	if pod.Status.PodIP == "" {
		logger.Info("Target pod does not have IP yet", "podName", podName, "podPhase", pod.Status.Phase)
		return "", fmt.Errorf("Target pod %s does not have an IP address yet", podName)
	}

	targetAddress := fmt.Sprintf("%s:%d", pod.Status.PodIP, VALKEY_PORT)
	logger.Info("Resolved target pod address",
		"podName", podName,
		"targetAddress", targetAddress)

	return targetAddress, nil
}

// ReshardSlots migrates slots from one node to another using a Kubernetes Job.
// This is the primary operation for rebalancing the cluster during scaling.
func (m *ValkeyJobManager) ReshardSlots(ctx context.Context, valkeyCluster *cachev1alpha1.ValkeyCluster, fromNodeID, toNodeID string, slotCount int) error {
	logger := log.FromContext(ctx)

	logger.Info("Starting slot resharding via Job",
		"fromNodeID", fromNodeID,
		"toNodeID", toNodeID,
		"slotCount", slotCount)

	targetAddress, err := m.getTargetPodAddress(ctx, valkeyCluster)
	if err != nil {
		return fmt.Errorf("Failed to get target pod address: %w", err)
	}

	args := []string{
		"--cluster", "reshard", targetAddress,
		"--cluster-from", fromNodeID,
		"--cluster-to", toNodeID,
		"--cluster-slots", fmt.Sprintf("%d", slotCount),
		"--cluster-yes",
	}

	stdout, stderr, err := m.reconciler.executeValkeyCliJob(ctx, valkeyCluster, args)

	if err != nil {
		logger.Error(err, "Failed to reshard slots",
			"fromNodeID", fromNodeID,
			"toNodeID", toNodeID,
			"slotCount", slotCount,
			"stdout", stdout,
			"stderr", stderr)
		return fmt.Errorf("Failed to reshard %d slots from %s to %s: %w", slotCount, fromNodeID, toNodeID, err)
	}

	logger.Info("Successfully resharded slots via Job",
		"fromNodeID", fromNodeID,
		"toNodeID", toNodeID,
		"slotCount", slotCount)

	return nil
}

// CheckCluster verifies the cluster health and slot distribution.
// Returns the check output which can be analyzed for issues.
func (m *ValkeyJobManager) CheckCluster(ctx context.Context, valkeyCluster *cachev1alpha1.ValkeyCluster) (string, error) {
	logger := log.FromContext(ctx)

	logger.Info("Checking cluster health via Job")

	targetAddress, err := m.getTargetPodAddress(ctx, valkeyCluster)
	if err != nil {
		return "", fmt.Errorf("Failed to get target pod address: %w", err)
	}

	args := []string{
		"--cluster", "check", targetAddress,
	}

	stdout, stderr, err := m.reconciler.executeValkeyCliJob(ctx, valkeyCluster, args)

	if err != nil {
		logger.Info("Cluster check Job returned error (may be non-fatal)",
			"error", err,
			"stdout", stdout,
			"stderr", stderr)
	}

	// Return stdout even on error as it contains diagnostic information
	return stdout, err
}

// FixClusterSlots attempts to fix stuck slots in the cluster.
// This is called when slots are in "migrating" or "importing" state and need manual intervention.
func (m *ValkeyJobManager) FixClusterSlots(ctx context.Context, valkeyCluster *cachev1alpha1.ValkeyCluster, logger logr.Logger) error {
	logger.Info("Attempting to fix stuck slots via Job")

	targetAddress, err := m.getTargetPodAddress(ctx, valkeyCluster)
	if err != nil {
		return fmt.Errorf("Failed to get target pod address: %w", err)
	}

	args := []string{
		"--cluster", "fix", targetAddress,
		"--cluster-fix-with-unreachable-primaries",
	}

	stdout, stderr, err := m.reconciler.executeValkeyCliJob(ctx, valkeyCluster, args)

	logger.Info("Cluster fix Job output",
		"stdout", stdout,
		"stderr", stderr)

	if err != nil {
		// Check if the cluster is down during fix
		if isClusterDown(stdout, stderr, err) {
			logger.Info("Cluster is down during fix attempt, caller should retry")
			return fmt.Errorf("cluster is down during fix attempt")
		}
		// Log the error but check if the fix actually succeeded despite error return
		logger.Info("Fix Job returned error, verifying if it actually fixed the issues",
			"error", err)
	}

	// Verify that slots are fixed by checking again
	checkStdout, checkErr := m.CheckCluster(ctx, valkeyCluster)
	if checkErr != nil {
		logger.Info("Check Job returned error after fix",
			"error", checkErr,
			"stdout", checkStdout)
	}

	if hasOpenSlots(checkStdout) {
		return fmt.Errorf("Failed to fix open slots in cluster: stdout: %s, stderr: %s, original error: %w", stdout, stderr, err)
	}

	logger.Info("Successfully fixed stuck slots via Job")
	return nil
}

// DeleteNode removes a node from the cluster.
// This is used during scale-down operations after slots have been migrated away.
func (m *ValkeyJobManager) DeleteNode(ctx context.Context, valkeyCluster *cachev1alpha1.ValkeyCluster, nodeID string) error {
	logger := log.FromContext(ctx)

	logger.Info("Deleting node from cluster via Job",
		"nodeID", nodeID)

	targetAddress, err := m.getTargetPodAddress(ctx, valkeyCluster)
	if err != nil {
		return fmt.Errorf("Failed to get target pod address: %w", err)
	}

	args := []string{
		"--cluster", "del-node",
		targetAddress,
		nodeID,
	}

	stdout, stderr, err := m.reconciler.executeValkeyCliJob(ctx, valkeyCluster, args)

	if err != nil {
		// Treat "No such node ID" as success - the node is already removed
		if strings.Contains(stdout, "No such node ID") || strings.Contains(stderr, "No such node ID") {
			logger.Info("Node already removed from cluster (No such node ID), treating as success",
				"nodeID", nodeID)
			return nil
		}
		logger.Error(err, "Failed to delete node from cluster",
			"nodeID", nodeID,
			"stdout", stdout,
			"stderr", stderr)
		return fmt.Errorf("Failed to delete node %s from cluster: %w", nodeID, err)
	}

	logger.Info("Successfully deleted node from cluster via Job",
		"nodeID", nodeID)

	return nil
}

// CheckForStuckSlots checks if the cluster has any open (stuck) slots.
// Returns true if stuck slots are detected.
func (m *ValkeyJobManager) CheckForStuckSlots(ctx context.Context, valkeyCluster *cachev1alpha1.ValkeyCluster) (bool, error) {
	logger := log.FromContext(ctx)

	stdout, err := m.CheckCluster(ctx, valkeyCluster)
	if err != nil {
		logger.Info("Check cluster returned error while checking for stuck slots",
			"error", err)
	}

	hasStuckSlots := hasOpenSlots(stdout)
	if hasStuckSlots {
		logger.Info("Detected stuck slots in cluster",
			"stdout", stdout)
	}

	return hasStuckSlots, err
}

// FixStuckSlotsIfNeeded checks for and fixes stuck slots if they exist.
// This is a convenience method that combines check and fix operations.
// Returns true if slots were stuck and fixed, false if no action was needed.
func (m *ValkeyJobManager) FixStuckSlotsIfNeeded(ctx context.Context, valkeyCluster *cachev1alpha1.ValkeyCluster, logger logr.Logger) (bool, error) {
	hasStuckSlots, checkErr := m.CheckForStuckSlots(ctx, valkeyCluster)

	if checkErr != nil {
		logger.Info("Check for stuck slots returned error",
			"error", checkErr)
	}

	if !hasStuckSlots {
		logger.Info("No stuck slots detected, skipping fix")
		return false, nil
	}

	logger.Info("Stuck slots detected, attempting to fix")

	if err := m.FixClusterSlots(ctx, valkeyCluster, logger); err != nil {
		return true, err
	}

	logger.Info("Successfully fixed stuck slots")
	return true, nil
}

// IsClusterDown checks if the error indicates the cluster is down.
// This is useful for determining if a retry is appropriate.
func (m *ValkeyJobManager) IsClusterDown(stdout, stderr string, err error) bool {
	return isClusterDown(stdout, stderr, err)
}
