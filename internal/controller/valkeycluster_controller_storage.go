package controller

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cachev1alpha1 "github.com/halter/valkey-cluster-operator/api/v1alpha1"
)

const (
	// defaultStorageSize is the initial volume size used when spec.storage does
	// not request one.
	defaultStorageSize = "1Gi"

	// valkeyDataMountPath is where the valkey-data volume is mounted in the
	// valkey-cluster-node container.
	valkeyDataMountPath = "/data"

	// diskUsageThresholdPercent is the usage of the fullest data volume in the
	// cluster at which all volumes are grown.
	diskUsageThresholdPercent = 50

	// diskGrowthPercent is how much volumes grow on each expansion, rounded up
	// to a whole Gi. Kept coarse so EBS-style backends, which allow only one
	// modification per volume per ~6h, gain meaningful headroom per step.
	diskGrowthPercent = 50

	// diskUsagePollInterval is how often a stable cluster is re-reconciled to
	// measure disk usage.
	diskUsagePollInterval = time.Minute
)

const gibibyte = int64(1024 * 1024 * 1024)

// desiredStorageSize returns the per-node volume size the cluster should have
// right now: the auto-scaled size from status when present, otherwise the size
// requested in spec.storage, otherwise the default initial size.
func desiredStorageSize(valkeyCluster *cachev1alpha1.ValkeyCluster) resource.Quantity {
	size := resource.MustParse(defaultStorageSize)
	if storage := valkeyCluster.Spec.Storage; storage != nil {
		if request, ok := storage.Resources.Requests[corev1.ResourceStorage]; ok && !request.IsZero() {
			size = request
		}
	}
	if scaled := valkeyCluster.Status.StorageSize; scaled != nil && scaled.Cmp(size) > 0 {
		size = *scaled
	}
	return size
}

// storagePVCSpec builds the effective PVC spec for the cluster's data volumes,
// applying defaults for anything not provided in spec.storage and the current
// auto-scaled size.
func storagePVCSpec(valkeyCluster *cachev1alpha1.ValkeyCluster) corev1.PersistentVolumeClaimSpec {
	spec := corev1.PersistentVolumeClaimSpec{
		AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
	}
	if storage := valkeyCluster.Spec.Storage; storage != nil {
		if len(storage.AccessModes) > 0 {
			spec.AccessModes = storage.AccessModes
		}
		spec.StorageClassName = storage.StorageClassName
		spec.Resources = *storage.Resources.DeepCopy()
	}
	if spec.Resources.Requests == nil {
		spec.Resources.Requests = corev1.ResourceList{}
	}
	spec.Resources.Requests[corev1.ResourceStorage] = desiredStorageSize(valkeyCluster)
	return spec
}

// nextStorageSize grows the current size by diskGrowthPercent, rounded up to a
// whole Gi.
func nextStorageSize(current resource.Quantity) resource.Quantity {
	grown := current.Value() + current.Value()*diskGrowthPercent/100
	gi := (grown + gibibyte - 1) / gibibyte
	return *resource.NewQuantity(gi*gibibyte, resource.BinarySI)
}

// parseDfUsedPercent parses POSIX `df -Pk <path>` output and returns the used
// capacity as a percentage. It is computed from the Used and Available columns
// rather than the Capacity column so reserved filesystem blocks do not skew
// the result.
func parseDfUsedPercent(out string) (int, error) {
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) < 2 {
		return 0, fmt.Errorf("unexpected df output: %q", out)
	}
	fields := strings.Fields(lines[len(lines)-1])
	if len(fields) < 5 {
		return 0, fmt.Errorf("unexpected df output line: %q", lines[len(lines)-1])
	}
	used, err := strconv.ParseInt(fields[2], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse df used blocks %q: %w", fields[2], err)
	}
	available, err := strconv.ParseInt(fields[3], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse df available blocks %q: %w", fields[3], err)
	}
	if used+available <= 0 {
		return 0, fmt.Errorf("df reports no capacity: %q", lines[len(lines)-1])
	}
	return int(used * 100 / (used + available)), nil
}

func (r *ValkeyClusterReconciler) podDiskUsedPercent(ctx context.Context, valkeyCluster *cachev1alpha1.ValkeyCluster, podName string) (int, error) {
	stdout, _, err := r.execInPod(ctx, valkeyCluster.Namespace, podName, []string{"df", "-Pk", valkeyDataMountPath})
	if err != nil {
		return 0, err
	}
	return parseDfUsedPercent(stdout)
}

// storageClassAllowsExpansion resolves the StorageClass bound to the PVC and
// reports whether it permits volume expansion.
func (r *ValkeyClusterReconciler) storageClassAllowsExpansion(ctx context.Context, pvc *corev1.PersistentVolumeClaim) (bool, string, error) {
	scName := ""
	if pvc.Spec.StorageClassName != nil {
		scName = *pvc.Spec.StorageClassName
	}
	if scName == "" {
		return false, scName, nil
	}
	sc := &storagev1.StorageClass{}
	if err := r.Get(ctx, types.NamespacedName{Name: scName}, sc); err != nil {
		return false, scName, err
	}
	return sc.AllowVolumeExpansion != nil && *sc.AllowVolumeExpansion, scName, nil
}

// reconcileDiskAutoScaling measures disk usage of every pod's data volume and,
// when the fullest volume crosses diskUsageThresholdPercent, grows the
// cluster-wide target size by diskGrowthPercent. All PVCs in the cluster share
// one target size, volumes only ever grow, and growth stops at
// spec.storageLimit when set. The new target is recorded in status; the PVC
// reconciliation applies it to each claim.
func (r *ValkeyClusterReconciler) reconcileDiskAutoScaling(ctx context.Context, req ctrl.Request, valkeyCluster *cachev1alpha1.ValkeyCluster) (*ctrl.Result, error) {
	logger := log.FromContext(ctx)

	desired := desiredStorageSize(valkeyCluster)
	listOpts := []client.ListOption{
		client.InNamespace(valkeyCluster.Namespace),
		client.MatchingLabels(labelsForValkeyCluster(valkeyCluster.Name)),
	}

	// Every PVC must have reached the current target capacity before another
	// expansion is considered, otherwise a slow in-flight expansion would
	// compound the growth factor on every reconcile.
	pvcList := &corev1.PersistentVolumeClaimList{}
	if err := r.List(ctx, pvcList, listOpts...); err != nil {
		return nil, err
	}
	if len(pvcList.Items) == 0 {
		return nil, nil
	}
	for _, pvc := range pvcList.Items {
		capacity, ok := pvc.Status.Capacity[corev1.ResourceStorage]
		if !ok || capacity.Cmp(desired) < 0 {
			logger.Info("Volume expansion in progress, skipping disk auto-scaling",
				"pvc", pvc.Name, "capacity", capacity.String(), "target", desired.String())
			return nil, nil
		}
	}

	podList := &corev1.PodList{}
	if err := r.List(ctx, podList, listOpts...); err != nil {
		return nil, err
	}
	maxUsedPercent := 0
	measured := false
	for _, pod := range podList.Items {
		if pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning {
			continue
		}
		usedPercent, err := r.podDiskUsedPercent(ctx, valkeyCluster, pod.Name)
		if err != nil {
			// Pods can churn between listing and exec; measure what we can and
			// let the periodic requeue retry the rest.
			logger.Error(err, "Failed to measure disk usage", "pod", pod.Name)
			continue
		}
		measured = true
		if usedPercent > maxUsedPercent {
			maxUsedPercent = usedPercent
		}
	}
	if !measured || maxUsedPercent < diskUsageThresholdPercent {
		return nil, nil
	}

	next := nextStorageSize(desired)
	if limit := valkeyCluster.Spec.StorageLimit; limit != nil && next.Cmp(*limit) > 0 {
		next = *limit
	}
	if next.Cmp(desired) <= 0 {
		return nil, r.setStorageLimitedCondition(ctx, req, valkeyCluster, metav1.Condition{
			Type: typeStorageLimitedValkeyCluster, Status: metav1.ConditionTrue, Reason: "StorageLimitReached",
			Message: fmt.Sprintf("Disk usage is %d%% but volume size %s already reached spec.storageLimit %s",
				maxUsedPercent, desired.String(), valkeyCluster.Spec.StorageLimit.String()),
		})
	}

	allowsExpansion, scName, err := r.storageClassAllowsExpansion(ctx, &pvcList.Items[0])
	if err != nil {
		return nil, err
	}
	if !allowsExpansion {
		return nil, r.setStorageLimitedCondition(ctx, req, valkeyCluster, metav1.Condition{
			Type: typeStorageLimitedValkeyCluster, Status: metav1.ConditionTrue, Reason: "StorageClassNotExpandable",
			Message: fmt.Sprintf("Disk usage is %d%% but StorageClass %q does not allow volume expansion",
				maxUsedPercent, scName),
		})
	}

	// Re-fetch before updating status to avoid conflicts with earlier updates
	// in this reconcile.
	if err := r.Get(ctx, req.NamespacedName, valkeyCluster); err != nil {
		return nil, err
	}
	valkeyCluster.Status.StorageSize = &next
	meta.SetStatusCondition(&valkeyCluster.Status.Conditions, metav1.Condition{
		Type: typeStorageLimitedValkeyCluster, Status: metav1.ConditionFalse, Reason: "Expanding",
		Message: fmt.Sprintf("Disk usage reached %d%%, growing volume size from %s to %s",
			maxUsedPercent, desired.String(), next.String()),
	})
	if err := r.Status().Update(ctx, valkeyCluster); err != nil {
		logger.Error(err, "Failed to update ValkeyCluster status with new storage size")
		return nil, err
	}
	r.Recorder.Event(valkeyCluster, "Normal", "DiskAutoScale",
		fmt.Sprintf("Disk usage reached %d%%, growing volume size from %s to %s",
			maxUsedPercent, desired.String(), next.String()))
	// Requeue so the PVC reconciliation applies the new size immediately.
	return &ctrl.Result{Requeue: true}, nil
}

// setStorageLimitedCondition records why auto-scaling cannot proceed, emitting
// a warning event only on transition to avoid spamming every poll.
func (r *ValkeyClusterReconciler) setStorageLimitedCondition(ctx context.Context, req ctrl.Request, valkeyCluster *cachev1alpha1.ValkeyCluster, condition metav1.Condition) error {
	if err := r.Get(ctx, req.NamespacedName, valkeyCluster); err != nil {
		return err
	}
	if !meta.SetStatusCondition(&valkeyCluster.Status.Conditions, condition) {
		return nil
	}
	if err := r.Status().Update(ctx, valkeyCluster); err != nil {
		return err
	}
	r.Recorder.Event(valkeyCluster, "Warning", condition.Reason, condition.Message)
	return nil
}
