package controller

import (
	"bytes"
	"context"
	"fmt"
	"strings"

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
	if err != nil {
		return "", "", fmt.Errorf("Failed executing command 'valkey-cli %s': stdout: %s, stderr: %s, err: %v", strings.Join(args, " "), stdout.String(), stderr.String(), err)
	}
	return stdout.String(), stderr.String(), nil
}
