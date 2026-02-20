package controller

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/go-logr/logr"
	cachev1alpha1 "github.com/halter/valkey-cluster-operator/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// executeValkeyCliJob runs a valkey-cli command using a Kubernetes Job instead of exec.
// This approach is more debuggable and doesn't consume resources on the valkey pods.
func (r *ValkeyClusterReconciler) executeValkeyCliJob(ctx context.Context, valkeyCluster *cachev1alpha1.ValkeyCluster, args []string) (string, string, error) {
	logger := log.FromContext(ctx)

	// Generate unique job name based on timestamp and operation
	jobName := fmt.Sprintf("%s-valkey-cli-%d", valkeyCluster.Name, time.Now().Unix())

	// Create the Job
	job := r.buildValkeyCliJob(jobName, valkeyCluster, args)

	logger.Info("Creating valkey-cli Job",
		"jobName", jobName,
		"args", args)

	if err := r.Create(ctx, job); err != nil {
		return "", "", fmt.Errorf("Failed to create valkey-cli Job: %w", err)
	}

	// Wait for Job to complete
	stdout, stderr, err := r.waitForJobCompletion(ctx, valkeyCluster.Namespace, jobName, logger)

	// Clean up the Job (best effort, don't fail if cleanup fails)
	if cleanupErr := r.deleteJob(ctx, valkeyCluster.Namespace, jobName, logger); cleanupErr != nil {
		logger.Info("Failed to cleanup Job (non-fatal)",
			"jobName", jobName,
			"error", cleanupErr)
	}

	return stdout, stderr, err
}

// buildValkeyCliJob creates a Job spec for running valkey-cli commands
func (r *ValkeyClusterReconciler) buildValkeyCliJob(jobName string, valkeyCluster *cachev1alpha1.ValkeyCluster, args []string) *batchv1.Job {
	// Build the valkey-cli command
	valkeyCliCmd := "valkey-cli"
	if valkeyCluster.Spec.Password != "" {
		valkeyCliCmd = fmt.Sprintf("%s -a %s", valkeyCliCmd, valkeyCluster.Spec.Password)
	}

	// Args already contain the correct target address from ValkeyJobManager
	fullCommand := fmt.Sprintf("%s %s", valkeyCliCmd, strings.Join(args, " "))

	// Job configuration
	backoffLimit := int32(0)               // Don't retry on failure
	ttlSecondsAfterFinished := int32(3600) // Keep job for 1 hour for debugging

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: valkeyCluster.Namespace,
			Labels: map[string]string{
				"app":      "valkey-cluster-operator",
				"cluster":  valkeyCluster.Name,
				"job-type": "valkey-cli",
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoffLimit,
			TTLSecondsAfterFinished: &ttlSecondsAfterFinished,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app":      "valkey-cluster-operator",
						"cluster":  valkeyCluster.Name,
						"job-type": "valkey-cli",
					},
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:            "valkey-cli",
							Image:           valkeyCluster.Spec.Image,
							ImagePullPolicy: corev1.PullIfNotPresent, // Use IfNotPresent for kind compatibility
							Command:         []string{"sh", "-c", fullCommand},
							// Use same resources as specified in the cluster, but with lower limits for the CLI
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    *parseQuantity("10m"),
									corev1.ResourceMemory: *parseQuantity("64Mi"),
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    *parseQuantity("100m"),
									corev1.ResourceMemory: *parseQuantity("128Mi"),
								},
							},
						},
					},
					// Apply the same node selector and tolerations as the cluster
					NodeSelector: valkeyCluster.Spec.NodeSelector,
					Tolerations:  valkeyCluster.Spec.Tolerations,
				},
			},
		},
	}

	return job
}

// waitForJobCompletion waits for a Job to complete and returns its output
func (r *ValkeyClusterReconciler) waitForJobCompletion(ctx context.Context, namespace, jobName string, logger logr.Logger) (string, string, error) {
	timeout := 5 * time.Minute // Reduced timeout to avoid test timeouts
	pollInterval := 2 * time.Second
	deadline := time.Now().Add(timeout)

	for {
		if time.Now().After(deadline) {
			return "", "", fmt.Errorf("Timeout waiting for Job %s to complete after %v", jobName, timeout)
		}

		// Get the Job
		job := &batchv1.Job{}
		err := r.Get(ctx, types.NamespacedName{Name: jobName, Namespace: namespace}, job)
		if err != nil {
			if apierrors.IsNotFound(err) {
				// Cache may not have synced yet after Job creation, retry
				logger.Info("Job not found in cache yet, retrying", "jobName", jobName)
				time.Sleep(pollInterval)
				continue
			}
			return "", "", fmt.Errorf("Failed to get Job %s: %w", jobName, err)
		}

		// Check if Job completed
		if job.Status.Succeeded > 0 {
			logger.Info("Job completed successfully", "jobName", jobName)
			return r.getJobLogs(ctx, namespace, jobName, logger)
		}

		if job.Status.Failed > 0 {
			logger.Info("Job failed", "jobName", jobName)
			stdout, stderr, _ := r.getJobLogs(ctx, namespace, jobName, logger)
			return stdout, stderr, fmt.Errorf("Job %s failed", jobName)
		}

		// Job still running, wait and retry
		time.Sleep(pollInterval)
	}
}

// getJobLogs retrieves the logs from a Job's pod
func (r *ValkeyClusterReconciler) getJobLogs(ctx context.Context, namespace, jobName string, logger logr.Logger) (string, string, error) {
	// List pods for this Job
	podList := &corev1.PodList{}
	err := r.List(ctx, podList, &client.ListOptions{
		Namespace: namespace,
		LabelSelector: labels.SelectorFromSet(map[string]string{
			"job-name": jobName,
		}),
	})
	if err != nil {
		logger.Error(err, "Failed to list pods for Job", "jobName", jobName)
		return "", "", fmt.Errorf("Failed to list pods for Job %s: %w", jobName, err)
	}

	if len(podList.Items) == 0 {
		logger.Info("No pods found for Job", "jobName", jobName)
		return "", "", fmt.Errorf("No pods found for Job %s", jobName)
	}

	// Get logs from the first pod (there should only be one)
	pod := podList.Items[0]

	logger.Info("Retrieving logs from Job pod",
		"jobName", jobName,
		"podName", pod.Name,
		"podPhase", pod.Status.Phase)

	// If pod hasn't started yet, we can't get logs
	if pod.Status.Phase == corev1.PodPending {
		logger.Info("Pod is still pending, cannot retrieve logs yet",
			"podName", pod.Name)
		return "", "", fmt.Errorf("Pod %s is still pending", pod.Name)
	}

	// Get logs
	req := r.ClientSet.CoreV1().Pods(namespace).GetLogs(pod.Name, &corev1.PodLogOptions{})
	logs, err := req.Stream(ctx)
	if err != nil {
		logger.Error(err, "Failed to get log stream from pod", "podName", pod.Name)
		return "", "", fmt.Errorf("Failed to get logs from pod %s: %w", pod.Name, err)
	}
	defer logs.Close()

	// Read all logs
	buf := new(strings.Builder)
	_, err = io.Copy(buf, logs)
	if err != nil {
		logger.Error(err, "Failed to read logs from pod", "podName", pod.Name)
		return "", "", fmt.Errorf("Failed to read logs from pod %s: %w", pod.Name, err)
	}

	output := buf.String()
	logger.Info("Retrieved logs from Job pod",
		"jobName", jobName,
		"podName", pod.Name,
		"logLength", len(output))

	// For now, we return all output as stdout
	// valkey-cli typically outputs to stdout, errors would be in the Job status
	return output, "", nil
}

// deleteJob deletes a Job and its associated pods
func (r *ValkeyClusterReconciler) deleteJob(ctx context.Context, namespace, jobName string, logger logr.Logger) error {
	job := &batchv1.Job{}
	err := r.Get(ctx, types.NamespacedName{Name: jobName, Namespace: namespace}, job)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil // Already deleted
		}
		return err
	}

	// Delete the Job with propagation policy to delete pods as well
	propagationPolicy := metav1.DeletePropagationForeground
	deleteOptions := &client.DeleteOptions{
		PropagationPolicy: &propagationPolicy,
	}

	if err := r.Delete(ctx, job, deleteOptions); err != nil {
		return fmt.Errorf("Failed to delete Job %s: %w", jobName, err)
	}

	logger.Info("Successfully deleted Job", "jobName", jobName)
	return nil
}

// parseQuantity is a helper to parse resource quantities
func parseQuantity(q string) *resource.Quantity {
	quantity := resource.MustParse(q)
	return &quantity
}
