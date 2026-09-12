/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package e2e

import (
	"fmt"
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/halter/valkey-cluster-operator/test/utils"
)

// Three shards, no replicas: FAIL promotion needs a majority of slot-holding
// masters (2 shards can never hard-fail a dead master), and without replicas
// no auto-failover can preempt the operator's slot-takeover path.
const (
	deadNodeClusterName   = "valkeycluster-deadnode"
	deadNodeClusterShards = 3
)

var _ = Describe("dead node recovery", Ordered, func() {
	BeforeAll(func() {
		By("creating manager namespace")
		cmd := exec.Command("kubectl", "create", "ns", namespace)
		_, _ = utils.Run(cmd)

		By("installing CRDs")
		cmd = exec.Command("make", "install")
		_, err := utils.Run(cmd)
		ExpectWithOffset(1, err).NotTo(HaveOccurred())

		By("deploying the controller-manager")
		projectimage := "valkey-cluster-operator:latest"
		cmd = exec.Command("make", "deploy", fmt.Sprintf("IMG=%s", projectimage))
		_, err = utils.Run(cmd)
		ExpectWithOffset(1, err).NotTo(HaveOccurred())

		By("waiting for controller-manager to be ready")
		verifyControllerUp := func() error {
			cmd = exec.Command("kubectl", "get",
				"pods", "-l", "control-plane=controller-manager",
				"-o", "go-template={{ range .items }}"+
					"{{ if not .metadata.deletionTimestamp }}"+
					"{{ .metadata.name }}"+
					"{{ \"\\n\" }}{{ end }}{{ end }}",
				"-n", namespace,
			)
			podOutput, err := utils.Run(cmd)
			if err != nil {
				return err
			}
			podNames := utils.GetNonEmptyLines(string(podOutput))
			if len(podNames) != 1 {
				return fmt.Errorf("expect 1 controller pods running, but got %d", len(podNames))
			}
			cmd = exec.Command("kubectl", "get",
				"pods", podNames[0], "-o", "jsonpath={.status.phase}",
				"-n", namespace,
			)
			status, err := utils.Run(cmd)
			if err != nil {
				return err
			}
			if string(status) != "Running" {
				return fmt.Errorf("controller pod in %s status", status)
			}
			return nil
		}
		EventuallyWithOffset(1, verifyControllerUp, time.Minute, 2*time.Second).Should(Succeed())
	})

	AfterAll(func() {
		By("cleaning up dead-node test resources")
		cmd := exec.Command("kubectl", "delete", "--timeout=30s", "valkeycluster", deadNodeClusterName,
			"-n", namespace,
		)
		_, _ = utils.Run(cmd)

		cmd = exec.Command("kubectl", "delete", "--timeout=30s", "pvc",
			"-l", fmt.Sprintf("cache/name=%s", deadNodeClusterName),
			"-n", namespace,
		)
		_, _ = utils.Run(cmd)

		cmd = exec.Command("kubectl", "delete", "--timeout=10s", "deployment", "valkey-cluster-operator-controller-manager",
			"-n", namespace,
		)
		_, _ = utils.Run(cmd)
	})

	It("should recover slots owned by a node whose pod and PVC were deleted", func() {
		By("creating a 3-shard cluster without replicas")
		cmd := exec.Command("kubectl", "apply", "-n", namespace, "-f", "-")
		cmd.Stdin = strings.NewReader(fmt.Sprintf(`apiVersion: cache.halter.io/v1alpha1
kind: ValkeyCluster
metadata:
  name: %s
spec:
  image: valkey-server:8.0.5
  shards: %d
  replicas: 0
  minReadySeconds: 5
  resources:
    limits:
      cpu: "0.4"
      memory: 314Mi
    requests:
      cpu: "0.2"
      memory: 214Mi
  storage:
    accessModes:
    - ReadWriteOnce
    resources:
      requests:
        storage: 2Gi
    storageClassName: standard
  initialDelaySeconds: 5
`, deadNodeClusterName, deadNodeClusterShards))
		_, err := utils.Run(cmd)
		ExpectWithOffset(1, err).NotTo(HaveOccurred())

		By("waiting for the cluster to be ready")
		EventuallyWithOffset(1, verifyClusterState(deadNodeClusterName, deadNodeClusterShards, 0, ""), 3*time.Minute, 15*time.Second).Should(Succeed())

		victimPod := deadNodeClusterName + "-1-0"
		deadNodeID, err := podClusterMyID(victimPod)
		ExpectWithOffset(1, err).NotTo(HaveOccurred())
		ExpectWithOffset(1, deadNodeID).To(HaveLen(40))

		By("deleting the pod together with its PVC so the replacement joins with a fresh node ID")
		cmd = exec.Command("kubectl", "-n", namespace, "delete", "pvc", "valkey-data-"+victimPod, "--wait=false")
		_, err = utils.Run(cmd)
		ExpectWithOffset(1, err).NotTo(HaveOccurred())
		cmd = exec.Command("kubectl", "-n", namespace, "delete", "pod", victimPod, "--wait=false")
		_, err = utils.Run(cmd)
		ExpectWithOffset(1, err).NotTo(HaveOccurred())

		By("waiting for the replacement pod to come up with a new node ID")
		verifyReplacement := func() error {
			newID, err := podClusterMyID(victimPod)
			if err != nil {
				return err
			}
			if newID == deadNodeID {
				return fmt.Errorf("pod %s still has node ID %s", victimPod, deadNodeID)
			}
			return nil
		}
		EventuallyWithOffset(1, verifyReplacement, 5*time.Minute, 10*time.Second).Should(Succeed())

		By("waiting for the operator to forget the dead node and restore slot coverage")
		EventuallyWithOffset(1, func() error {
			return eventWithReasonExists(deadNodeClusterName, "DeadNodeForgotten")
		}, 5*time.Minute, 10*time.Second).Should(Succeed())
		EventuallyWithOffset(1, func() error {
			return eventWithReasonExists(deadNodeClusterName, "SlotCoverageRestored")
		}, 3*time.Minute, 10*time.Second).Should(Succeed())

		By("waiting for the cluster to fully converge with the dead node gone")
		verifyRecovered := func() error {
			if err := verifyClusterState(deadNodeClusterName, deadNodeClusterShards, 0, "")(); err != nil {
				return err
			}
			cmd := exec.Command("kubectl", "get",
				"pods", "-l", fmt.Sprintf("cache/name=%s", deadNodeClusterName),
				"-o", "go-template={{ range .items }}"+
					"{{ if not .metadata.deletionTimestamp }}"+
					"{{ .metadata.name }}"+
					"{{ \"\\n\" }}{{ end }}{{ end }}",
				"-n", namespace,
			)
			podOutput, err := utils.Run(cmd)
			if err != nil {
				return fmt.Errorf("received error getting pods: %w", err)
			}
			for _, pod := range utils.GetNonEmptyLines(string(podOutput)) {
				cmd = exec.Command("kubectl", "-n", namespace, "exec", pod, "-c", "valkey-cluster-node", "--",
					"valkey-cli", "cluster", "nodes")
				clusterNodesTxt, _, err := utils.RunWithSplitOutput(cmd)
				if err != nil {
					return fmt.Errorf("received error running kubectl exec: %w", err)
				}
				if strings.Contains(string(clusterNodesTxt), deadNodeID) {
					return fmt.Errorf("pod %s still lists dead node %s: %s", pod, deadNodeID, clusterNodesTxt)
				}
				if strings.Contains(string(clusterNodesTxt), "fail") {
					return fmt.Errorf("pod %s still lists a failed node: %s", pod, clusterNodesTxt)
				}
			}
			return nil
		}
		EventuallyWithOffset(1, verifyRecovered, 8*time.Minute, 15*time.Second).Should(Succeed())
	})
})

func podClusterMyID(podName string) (string, error) {
	cmd := exec.Command("kubectl", "-n", namespace, "exec", podName, "-c", "valkey-cluster-node", "--",
		"valkey-cli", "cluster", "myid")
	stdout, _, err := utils.RunWithSplitOutput(cmd)
	if err != nil {
		return "", fmt.Errorf("received error running CLUSTER MYID on pod %s: %w", podName, err)
	}
	return strings.TrimSpace(string(stdout)), nil
}
