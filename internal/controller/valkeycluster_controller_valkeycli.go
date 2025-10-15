package controller

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/go-logr/logr"
	cachev1alpha1 "github.com/halter/valkey-cluster-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/remotecommand"
)

func (r *ValkeyClusterReconciler) executeValkeyCli(ctx context.Context, valkeyCluster *cachev1alpha1.ValkeyCluster, args []string) (string, string, error) {
	valkeyCliCmd := "valkey-cli"
	if valkeyCluster.Spec.Password != "" {
		valkeyCliCmd = valkeyCliCmd + " -a " + valkeyCluster.Spec.Password
	}
	cmd := []string{
		"sh",
		"-c",
		fmt.Sprintf("%s %s", valkeyCliCmd, strings.Join(args, " ")),
	}

	podName := fmt.Sprintf("%s-0-0", valkeyCluster.Name)
	req := r.ClientSet.CoreV1().RESTClient().Post().Resource("pods").Name(podName).
		Namespace(valkeyCluster.Namespace).SubResource("exec")
	req.VersionedParams(&corev1.PodExecOptions{
		Container: "valkey-cluster-node",
		Command:   cmd,
		Stdin:     false,
		Stdout:    true,
		Stderr:    true,
		TTY:       false,
	}, runtime.NewParameterCodec(r.Scheme))
	exec, err := remotecommand.NewSPDYExecutor(r.RestConfig, "POST", req.URL())
	if err != nil {
		return "", "", fmt.Errorf("Failed to execute valkey-cli %s: %v", strings.Join(args, " "), err)
	}
	var stdout, stderr bytes.Buffer
	err = exec.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: &stdout,
		Stderr: &stderr,
	})
	// Return stdout and stderr even on error, as they contain useful diagnostic information
	stdoutStr := stdout.String()
	stderrStr := stderr.String()
	if err != nil {
		return stdoutStr, stderrStr, fmt.Errorf("Failed executing command 'valkey-cli %s': stdout: %s, stderr: %s, err: %v", strings.Join(args, " "), stdoutStr, stderrStr, err)
	}
	return stdoutStr, stderrStr, nil
}

// hasOpenSlots checks if the cluster has any open (stuck) slots by examining the check output
func hasOpenSlots(stdout string) bool {
	// Look for warning indicators of open slots
	return strings.Contains(stdout, "slots in migrating state") ||
		strings.Contains(stdout, "slots in importing state") ||
		strings.Contains(stdout, "The following slots are open:")
}

// isClusterDown checks if the error indicates the cluster is down
func isClusterDown(stdout, stderr string, err error) bool {
	if err == nil {
		return false
	}
	// Check for cluster down errors in various outputs
	clusterDownMsg := "CLUSTERDOWN The cluster is down"
	return strings.Contains(stdout, clusterDownMsg) ||
		strings.Contains(stderr, clusterDownMsg) ||
		(err != nil && strings.Contains(err.Error(), clusterDownMsg))
}

// fixClusterSlots attempts to fix stuck slots in the cluster using valkey-cli --cluster fix
func (r *ValkeyClusterReconciler) fixClusterSlots(ctx context.Context, valkeyCluster *cachev1alpha1.ValkeyCluster, logger logr.Logger) error {
	logger.Info("Attempting to fix stuck slots in cluster")

	stdout, stderr, err := r.executeValkeyCli(ctx, valkeyCluster, []string{"--cluster", "fix", "127.0.0.1:6379", "--cluster-fix-with-unreachable-primaries"})

	logger.Info("Cluster fix command output",
		"stdout", stdout,
		"stderr", stderr)

	if err != nil {
		// Log the error but check if the fix actually succeeded despite error return
		logger.Info("Fix command returned error, checking if it actually fixed the issues",
			"error", err)
	}

	// Verify that slots are fixed by checking again
	checkStdout, _, checkErr := r.executeValkeyCli(ctx, valkeyCluster, []string{"--cluster", "check", "127.0.0.1:6379"})
	if checkErr != nil {
		// check command may return error even when cluster is healthy if there are warnings
		logger.Info("Check command returned error after fix",
			"error", checkErr,
			"stdout", checkStdout)
	}

	if hasOpenSlots(checkStdout) {
		return fmt.Errorf("Failed to fix open slots in cluster: stdout: %s, stderr: %s, original error: %v", stdout, stderr, err)
	}

	logger.Info("Successfully fixed stuck slots in cluster")
	return nil
}
