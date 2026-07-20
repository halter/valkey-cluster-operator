package controller

import (
	"context"
	"crypto/sha256"
	"errors"
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

// errValkeyCliJobStillRunning indicates a valkey-cli Job is still running: either
// the Job this call created outlived the wait timeout, or a Job left behind by a
// previous reconcile has not finished yet. The Job is intentionally left running —
// deleting it would kill valkey-cli mid-operation (e.g. a slot migration) and leave
// the cluster with stuck slots. Callers should requeue and let a later reconcile
// pick up from wherever the Job got to.
var errValkeyCliJobStillRunning = errors.New("valkey-cli Job is still running")

// executeValkeyCliJob runs a valkey-cli command using a Kubernetes Job instead of exec.
// This approach is more debuggable and doesn't consume resources on the valkey pods.
//
// Only one valkey-cli Job is allowed per cluster at a time: concurrent jobs could
// race each other (e.g. a "--cluster fix" running against a live "--cluster reshard").
// If a Job from a previous reconcile is still running, this returns
// errValkeyCliJobStillRunning without creating a new Job.
func (r *ValkeyClusterReconciler) executeValkeyCliJob(ctx context.Context, valkeyCluster *cachev1alpha1.ValkeyCluster, args []string) (string, string, error) {
	logger := log.FromContext(ctx)

	// Don't start a competing Job while one is still running for this cluster
	// (reaping any finished leftovers from previous reconciles on the way).
	running, err := r.settleValkeyCliJobs(ctx, valkeyCluster, logger)
	if err != nil {
		return "", "", fmt.Errorf("Failed to check for running valkey-cli Jobs: %w", err)
	}
	if running != nil {
		logger.Info("A valkey-cli Job for this cluster is still running, not creating another",
			"runningJobName", running.Name,
			"args", args)
		return "", "", fmt.Errorf("%w: %s", errValkeyCliJobStillRunning, running.Name)
	}

	// Generate unique job name based on timestamp and operation
	jobName := valkeyCliJobName(valkeyCluster.Name, time.Now().Unix())

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

	// If the Job is still running (wait timed out) or we can no longer tell
	// (context cancelled, e.g. operator shutdown), leave it alone: deleting it
	// would kill valkey-cli mid-operation. A later reconcile adopts or reaps it
	// via settleValkeyCliJobs.
	if errors.Is(err, errValkeyCliJobStillRunning) || ctx.Err() != nil {
		logger.Info("Leaving valkey-cli Job running for a later reconcile to pick up",
			"jobName", jobName,
			"error", err)
		return stdout, stderr, err
	}

	// Clean up the Job (best effort, don't fail if cleanup fails)
	if cleanupErr := r.deleteJob(ctx, valkeyCluster.Namespace, jobName, logger); cleanupErr != nil {
		logger.Info("Failed to cleanup Job (non-fatal)",
			"jobName", jobName,
			"error", cleanupErr)
	}

	return stdout, stderr, err
}

// valkeyCliJobLabels returns the labels applied to valkey-cli Jobs for the given
// cluster, also used to find them again on later reconciles.
func valkeyCliJobLabels(clusterName string) map[string]string {
	return map[string]string{
		"app":      "valkey-cluster-operator",
		"cluster":  clusterName,
		"job-type": "valkey-cli",
	}
}

// isJobFinished reports whether a Job has run to completion (successfully or not).
func isJobFinished(job *batchv1.Job) bool {
	return job.Status.Succeeded > 0 || job.Status.Failed > 0
}

// leftoverJobLogTailBytes caps how much of a leftover Job's output is copied into
// the operator's own log. Reshard Jobs that outlived the wait are exactly the ones
// with large output, and the tail is what shows how the operation ended.
const leftoverJobLogTailBytes = 4096

// settleValkeyCliJobs looks for valkey-cli Jobs left behind by previous reconciles
// (jobs that outlived waitForJobCompletion's timeout, or that were running when the
// operator restarted). Finished leftovers have their output logged and are deleted.
// If a leftover Job is still running it is returned so the caller can requeue and
// wait for it instead of starting competing cluster operations.
func (r *ValkeyClusterReconciler) settleValkeyCliJobs(ctx context.Context, valkeyCluster *cachev1alpha1.ValkeyCluster, logger logr.Logger) (*batchv1.Job, error) {
	jobList := &batchv1.JobList{}
	err := r.List(ctx, jobList,
		client.InNamespace(valkeyCluster.Namespace),
		client.MatchingLabels(valkeyCliJobLabels(valkeyCluster.Name)))
	if err != nil {
		return nil, fmt.Errorf("Failed to list valkey-cli Jobs: %w", err)
	}

	var running *batchv1.Job
	for i := range jobList.Items {
		job := &jobList.Items[i]
		if !isJobFinished(job) {
			logger.Info("Found valkey-cli Job from a previous reconcile that is still running",
				"jobName", job.Name)
			running = job
			continue
		}

		// Finished leftover: capture its output for debugging, then reap it.
		stdout, _, logsErr := r.getJobLogs(ctx, valkeyCluster.Namespace, job.Name, logger)
		if logsErr != nil {
			logger.Info("Could not retrieve logs from finished leftover valkey-cli Job (non-fatal)",
				"jobName", job.Name,
				"error", logsErr)
		}
		if len(stdout) > leftoverJobLogTailBytes {
			stdout = "(truncated)..." + stdout[len(stdout)-leftoverJobLogTailBytes:]
		}
		logger.Info("Reaping finished valkey-cli Job from a previous reconcile",
			"jobName", job.Name,
			"succeeded", job.Status.Succeeded,
			"failed", job.Status.Failed,
			"stdout", stdout)
		if job.DeletionTimestamp == nil {
			if err := r.deleteJob(ctx, valkeyCluster.Namespace, job.Name, logger); err != nil {
				logger.Info("Failed to delete finished leftover valkey-cli Job (non-fatal)",
					"jobName", job.Name,
					"error", err)
			}
		}
	}

	return running, nil
}

// maxJobNameLength caps Job names at 63 characters: Kubernetes copies the Job
// name into the job-name label on the Job's pods, and label values are limited
// to 63 characters.
const maxJobNameLength = 63

// valkeyCliJobName returns "<cluster>-valkey-cli-<timestamp>", truncating long
// cluster names and appending a short hash of the full name so that names stay
// unique across clusters that share a truncated prefix.
func valkeyCliJobName(clusterName string, timestamp int64) string {
	suffix := fmt.Sprintf("-valkey-cli-%d", timestamp)
	maxPrefix := maxJobNameLength - len(suffix)
	if len(clusterName) <= maxPrefix {
		return clusterName + suffix
	}
	hash := fmt.Sprintf("%x", sha256.Sum256([]byte(clusterName)))[:8]
	prefix := strings.TrimRight(clusterName[:maxPrefix-len(hash)-1], "-")
	return fmt.Sprintf("%s-%s%s", prefix, hash, suffix)
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
			Labels:    valkeyCliJobLabels(valkeyCluster.Name),
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoffLimit,
			TTLSecondsAfterFinished: &ttlSecondsAfterFinished,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: valkeyCliJobLabels(valkeyCluster.Name),
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

// waitForJobCompletion waits for a Job to complete and returns its output.
//
// The timeout bounds how long a single reconcile blocks on the Job, not how long
// the Job may run: on timeout this returns errValkeyCliJobStillRunning and the
// Job keeps running. Long operations (e.g. reshard steps migrating large keyspaces)
// are picked up again by later reconciles via settleValkeyCliJobs.
func (r *ValkeyClusterReconciler) waitForJobCompletion(ctx context.Context, namespace, jobName string, logger logr.Logger) (string, string, error) {
	timeout := 5 * time.Minute // Reduced timeout to avoid test timeouts
	pollInterval := 2 * time.Second
	deadline := time.Now().Add(timeout)

	for {
		if time.Now().After(deadline) {
			return "", "", fmt.Errorf("%w: gave up waiting for Job %s after %v", errValkeyCliJobStillRunning, jobName, timeout)
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
		if isJobFinished(job) {
			if job.Status.Succeeded > 0 {
				logger.Info("Job completed successfully", "jobName", jobName)
				return r.getJobLogs(ctx, namespace, jobName, logger)
			}

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
