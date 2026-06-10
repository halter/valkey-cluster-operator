package controller

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cachev1alpha1 "github.com/halter/valkey-cluster-operator/api/v1alpha1"
)

// reconcileAuth live-applies the desired authentication settings to every
// running pod via CONFIG SET. The config file only takes effect on restart,
// and the rolling update is gated on replication health; without the live
// update, a restarted replica (which sends AUTH from primaryauth) cannot
// authenticate to a not-yet-restarted primary (which has no requirepass yet),
// its replication link stays down and the rollout deadlocks. Applying the
// password to all pods up front keeps replication authenticated in every
// mixed-config state; the rolling restart then merely makes it persistent.
func (r *ValkeyClusterReconciler) reconcileAuth(ctx context.Context, valkeyCluster *cachev1alpha1.ValkeyCluster) error {
	logger := log.FromContext(ctx)
	password := valkeyCluster.Spec.Password

	podList := &corev1.PodList{}
	listOpts := []client.ListOption{
		client.InNamespace(valkeyCluster.Namespace),
		client.MatchingLabels(labelsForValkeyCluster(valkeyCluster.Name)),
	}
	if err := r.List(ctx, podList, listOpts...); err != nil {
		return err
	}
	for _, pod := range podList.Items {
		if pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning || pod.Status.PodIP == "" {
			continue
		}
		valkeyClient, err := r.NewValkeyClient(ctx, valkeyCluster, pod.Status.PodIP, VALKEY_PORT)
		if err != nil {
			return fmt.Errorf("failed to create valkey client for pod %s: %w", pod.Name, err)
		}
		defer valkeyClient.Close()

		current, err := valkeyClient.Do(ctx, valkeyClient.B().ConfigGet().Parameter("requirepass").Build()).AsStrMap()
		if err != nil {
			return fmt.Errorf("failed to get requirepass from pod %s: %w", pod.Name, err)
		}
		if current["requirepass"] == password {
			continue
		}
		err = valkeyClient.Do(ctx, valkeyClient.B().ConfigSet().
			ParameterValue().
			ParameterValue("requirepass", password).
			ParameterValue("primaryauth", password).
			Build()).Error()
		if err != nil {
			return fmt.Errorf("failed to set auth config on pod %s: %w", pod.Name, err)
		}
		logger.Info("Applied authentication config to running pod", "pod", pod.Name)
		r.Recorder.Event(valkeyCluster, "Normal", "AuthUpdated",
			fmt.Sprintf("Applied authentication config to pod %s", pod.Name))
	}
	return nil
}
