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
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/halter/valkey-cluster-operator/internal/controller/valkey"
	"github.com/halter/valkey-cluster-operator/test/utils"
)

const namespace = "valkey-cluster-operator-system"

var _ = Describe("controller", Ordered, func() {
	BeforeAll(func() {
		By("creating manager namespace")
		cmd := exec.Command("kubectl", "create", "ns", namespace)
		_, _ = utils.Run(cmd)
	})

	AfterAll(func() {
		By("remove valkey cluster")
		cmd := exec.Command("kubectl", "delete", "--timeout=10s", "valkeycluster", "valkeycluster-sample",
			"-n", namespace,
		)
		_, _ = utils.Run(cmd)

		cmd = exec.Command("kubectl", "delete", "--timeout=10s", "pvc",
			"-l", fmt.Sprintf("cache/name=%s,app.kubernetes.io/name=valkeyCluster-operator,app.kubernetes.io/managed-by=ValkeyClusterController", "valkeycluster-sample"),
			"-n", namespace,
		)
		_, _ = utils.Run(cmd)

		cmd = exec.Command("kubectl", "delete", "--timeout=10s", "deployment", "valkey-cluster-operator-controller-manager",
			"-n", namespace,
		)
		_, _ = utils.Run(cmd)
	})

	Context("Operator", func() {
		It("should run successfully", func() {
			var controllerPodName string
			var err error
			projectDir, _ := utils.GetProjectDir()

			// projectimage stores the name of the image used in the example
			projectimage := "valkey-cluster-operator:latest"

			By("installing CRDs")
			cmd := exec.Command("make", "install")
			_, err = utils.Run(cmd)
			ExpectWithOffset(1, err).NotTo(HaveOccurred())

			By("deploying the controller-manager")
			cmd = exec.Command("make", "deploy", fmt.Sprintf("IMG=%s", projectimage))
			makeDeployOutput, err := utils.Run(cmd)
			_, _ = fmt.Fprintf(GinkgoWriter, "make deploy: %s\n", makeDeployOutput)
			ExpectWithOffset(1, err).NotTo(HaveOccurred())

			By("validating that the controller-manager pod is running as expected")
			verifyControllerUp := func() error {
				// Get pod name
				cmd = exec.Command("kubectl", "get",
					"pods", "-l", "control-plane=controller-manager",
					"-o", "go-template={{ range .items }}"+
						"{{ if not .metadata.deletionTimestamp }}"+
						"{{ .metadata.name }}"+
						"{{ \"\\n\" }}{{ end }}{{ end }}",
					"-n", namespace,
				)

				podOutput, err := utils.Run(cmd)
				ExpectWithOffset(2, err).NotTo(HaveOccurred())
				podNames := utils.GetNonEmptyLines(string(podOutput))
				if len(podNames) != 1 {
					return fmt.Errorf("expect 1 controller pods running, but got %d", len(podNames))
				}
				controllerPodName = podNames[0]
				ExpectWithOffset(2, controllerPodName).Should(ContainSubstring("controller-manager"))

				// Validate pod status
				cmd = exec.Command("kubectl", "get",
					"pods", controllerPodName, "-o", "jsonpath={.status.phase}",
					"-n", namespace,
				)
				status, err := utils.Run(cmd)
				ExpectWithOffset(2, err).NotTo(HaveOccurred())
				if string(status) != "Running" {
					cmd = exec.Command("kubectl", "describe",
						"pods", controllerPodName,
						"-n", namespace,
					)
					output, err := utils.Run(cmd)
					if err == nil {
						fmt.Printf("pod description: %s\n", string(output))
					}
					return fmt.Errorf("controller pod in %s status", status)
				}
				return nil
			}
			EventuallyWithOffset(1, verifyControllerUp, time.Minute, 2*time.Second).Should(Succeed())

			By("creating an instance of the ValkeyCluster Operand(CR)")
			EventuallyWithOffset(1, func() error {
				cmd = exec.Command("kubectl", "apply", "-f", filepath.Join(projectDir,
					"config/samples/cache_v1alpha1_valkeycluster_kind_e2e.yaml"), "-n", namespace)
				_, err = utils.Run(cmd)
				return err
			}, time.Minute, time.Second).Should(Succeed())

			By("validating that pod(s) status.phase=Running")
			getValkeyClusterPodStatus := func() error {
				cmd = exec.Command("kubectl", "get",
					"pods", "-l", "app.kubernetes.io/name=valkeyCluster-operator",
					"-o", "jsonpath={.items[*].status}", "-n", namespace,
				)
				status, err := utils.Run(cmd)
				fmt.Println(string(status))
				ExpectWithOffset(2, err).NotTo(HaveOccurred())
				if !strings.Contains(string(status), "\"phase\":\"Running\"") {
					return fmt.Errorf("valkeycluster pod in %s status", status)
				}
				return nil
			}
			EventuallyWithOffset(1, getValkeyClusterPodStatus, time.Minute, 15*time.Second).Should(Succeed())

			By("validating that the status of the custom resource created is updated or not")
			getStatus := func() error {
				cmd = exec.Command("kubectl", "get", "valkeycluster",
					"valkeycluster-sample", "-o", "jsonpath={.status.conditions}",
					"-n", namespace,
				)
				status, err := utils.Run(cmd)
				fmt.Println(string(status))
				ExpectWithOffset(2, err).NotTo(HaveOccurred())
				if !strings.Contains(string(status), "Available") {
					return fmt.Errorf("status condition with type Available should be set")
				}
				return nil
			}
			Eventually(getStatus, time.Minute, time.Second).Should(Succeed())

		})
		It("should have working cluster", func() {
			EventuallyWithOffset(1, verifyClusterState("valkeycluster-sample", 1, 1), 2*time.Minute, 15*time.Second).Should(Succeed())
		})
		It("should scale up", func() {
			cmd := exec.Command("kubectl",
				"-n", namespace,
				"patch", "valkeycluster", "valkeycluster-sample",
				"--type=json",
				`-p=[{"op":"replace","path":"/spec/shards","value":3}]`,
			)
			_, err := utils.Run(cmd)
			ExpectWithOffset(1, err).NotTo(HaveOccurred())
			EventuallyWithOffset(1, verifyClusterState("valkeycluster-sample", 3, 1), 5*time.Minute, 15*time.Second).Should(Succeed())
		})
		It("should scale down", func() {
			cmd := exec.Command("kubectl",
				"-n", namespace,
				"patch", "valkeycluster", "valkeycluster-sample",
				"--type=json",
				`-p=[{"op":"replace","path":"/spec/shards","value":2}]`,
			)
			_, err := utils.Run(cmd)
			ExpectWithOffset(1, err).NotTo(HaveOccurred())
			EventuallyWithOffset(1, verifyClusterState("valkeycluster-sample", 2, 1), 6*time.Minute, 15*time.Second).Should(Succeed())
			getPods := func() error {
				cmd = exec.Command("kubectl", "get", "pods",
					"-l", fmt.Sprintf("cache/name=%s,app.kubernetes.io/name=valkeyCluster-operator,app.kubernetes.io/managed-by=ValkeyClusterController", "valkeycluster-sample"),
					"-o", "go-template={{ range .items }}"+
						"{{ if not .metadata.deletionTimestamp }}"+
						"{{ .metadata.name }}"+
						"{{ \"\\n\" }}{{ end }}{{ end }}",
					"-n", namespace,
				)

				podOutput, err := utils.Run(cmd)
				ExpectWithOffset(2, err).NotTo(HaveOccurred())
				podNames := utils.GetNonEmptyLines(string(podOutput))
				if len(podNames) != 4 {
					return fmt.Errorf("expect 4 cache pods running, but got %d", len(podNames))
				}
				return nil
			}
			Eventually(getPods, time.Minute, time.Second).Should(Succeed())
			getPvc := func() error {
				cmd = exec.Command("kubectl", "get", "pvc",
					"-l", fmt.Sprintf("cache/name=%s,app.kubernetes.io/name=valkeyCluster-operator,app.kubernetes.io/managed-by=ValkeyClusterController", "valkeycluster-sample"),
					"-o", "go-template={{ range .items }}"+
						"{{ if not .metadata.deletionTimestamp }}"+
						"{{ .metadata.name }}"+
						"{{ \"\\n\" }}{{ end }}{{ end }}",
					"-n", namespace,
				)

				pvcOutput, err := utils.Run(cmd)
				ExpectWithOffset(2, err).NotTo(HaveOccurred())
				pvcNames := utils.GetNonEmptyLines(string(pvcOutput))
				if len(pvcNames) != 4 {
					return fmt.Errorf("expect 4 PVCs, but got %d", len(pvcNames))
				}
				return nil
			}
			Eventually(getPvc, time.Minute, time.Second).Should(Succeed())
		})
		It("should add replicas", func() {
			cmd := exec.Command("kubectl",
				"-n", namespace,
				"patch", "valkeycluster", "valkeycluster-sample",
				"--type=json",
				`-p=[{"op":"replace","path":"/spec/replicas","value":2}]`,
			)
			_, err := utils.Run(cmd)
			ExpectWithOffset(1, err).NotTo(HaveOccurred())
			EventuallyWithOffset(1, verifyClusterState("valkeycluster-sample", 2, 2), 3*time.Minute, 15*time.Second).Should(Succeed())
		})
		It("change resources", func() {
			cmd := exec.Command("kubectl",
				"-n", namespace,
				"patch", "valkeycluster", "valkeycluster-sample",
				"--type=json",
				`-p=[{"op":"replace","path":"/spec/resources","value":{"limits":{"cpu":"0.8","memory":"628Mi"},"requests":{"cpu":"0.4","memory":"228Mi"}}}]`,
			)
			_, err := utils.Run(cmd)
			ExpectWithOffset(1, err).NotTo(HaveOccurred())

			verifyPodResources := func() error {
				cmd = exec.Command("kubectl", "get", "pods",
					"-l", fmt.Sprintf("cache/name=%s,app.kubernetes.io/name=valkeyCluster-operator,app.kubernetes.io/managed-by=ValkeyClusterController", "valkeycluster-sample"),
					"-o", "go-template={{ range .items }}"+
						"{{ if not .metadata.deletionTimestamp }}"+
						"{{ .metadata.name }}"+
						"{{ \"\\n\" }}{{ end }}{{ end }}",
					"-n", namespace,
				)
				podOutput, err := utils.Run(cmd)
				ExpectWithOffset(2, err).NotTo(HaveOccurred())
				podNames := utils.GetNonEmptyLines(string(podOutput))

				for _, podName := range podNames {
					cmd = exec.Command("kubectl", "get", "pod", podName,
						"-o", "jsonpath={.spec.containers[0].resources}",
						"-n", namespace,
					)
					resourceOutput, err := utils.Run(cmd)
					ExpectWithOffset(2, err).NotTo(HaveOccurred())

					type Resources struct {
						Requests struct {
							CPU    string `json:"cpu"`
							Memory string `json:"memory"`
						} `json:"requests"`
						Limits struct {
							CPU    string `json:"cpu"`
							Memory string `json:"memory"`
						} `json:"limits"`
					}

					r := Resources{}
					err = json.Unmarshal(resourceOutput, &r)
					ExpectWithOffset(2, err).NotTo(HaveOccurred())

					if r.Requests.CPU != "400m" {
						return fmt.Errorf("expected CPU request to be 400m for pod %s but got %s", podName, r.Requests.CPU)
					}
					if r.Requests.Memory != "228Mi" {
						return fmt.Errorf("expected memory request to be 228Mi for pod %s but got %s", podName, r.Requests.Memory)
					}
					if r.Limits.CPU != "800m" {
						return fmt.Errorf("expected CPU limit to be 800m for pod %s but got %s", podName, r.Limits.CPU)
					}
					if r.Limits.Memory != "628Mi" {
						return fmt.Errorf("expected memory limit to be 628Mi for pod %s but got %s", podName, r.Limits.Memory)
					}
				}
				return nil
			}
			EventuallyWithOffset(1, verifyPodResources, 10*time.Minute, 15*time.Second).Should(Succeed())
		})
		It("change minReadySeconds", func() {
			verifyClusterMinReadySeconds := func() error {
				cmd := exec.Command("kubectl",
					"-n", namespace,
					"get", "valkeycluster",
					"valkeycluster-sample", "-o", "jsonpath={.spec.minReadySeconds}",
				)
				minReadySeconds, err := utils.Run(cmd)
				fmt.Println(string(minReadySeconds))
				ExpectWithOffset(2, err).NotTo(HaveOccurred())
				if string(minReadySeconds) != "10" {
					return fmt.Errorf("expected minReadySeconds value of 10 for valkeycluster but got %s", minReadySeconds)
				}
				return nil
			}
			Eventually(verifyClusterMinReadySeconds, time.Minute, time.Second).Should(Succeed())

			verifyStatefulSetMinReadySeconds := func() error {
				cmd := exec.Command("kubectl",
					"-n", namespace,
					"get", "statefulset",
					"-l", fmt.Sprintf("cache/name=%s,app.kubernetes.io/name=valkeyCluster-operator,app.kubernetes.io/managed-by=ValkeyClusterController", "valkeycluster-sample"),
					"-o", "go-template={{ range .items }}"+
						"{{ .spec.minReadySeconds }}"+
						"{{ \"\\n\" }}{{ end }}",
				)
				statefulSetOutput, err := utils.Run(cmd)
				ExpectWithOffset(2, err).NotTo(HaveOccurred())
				statefulSetMinReadyValues := utils.GetNonEmptyLines(string(statefulSetOutput))
				if len(statefulSetMinReadyValues) != 2 {
					return fmt.Errorf("expected 2 statefulsets running, but got %d", len(statefulSetMinReadyValues))
				}
				for _, v := range statefulSetMinReadyValues {
					if v != "10" {
						return fmt.Errorf("expected minReadySeconds value of 10 for statefulset but got %s", v)
					}
				}
				return nil
			}
			Eventually(verifyStatefulSetMinReadySeconds, time.Minute, time.Second).Should(Succeed())

			patchCmd := exec.Command("kubectl",
				"-n", namespace,
				"patch", "valkeycluster", "valkeycluster-sample",
				"--type=json",
				`-p=[{"op":"replace","path":"/spec/minReadySeconds","value":15}]`,
			)
			_, err := utils.Run(patchCmd)
			ExpectWithOffset(1, err).NotTo(HaveOccurred())

			reverifyClusterMinReadySeconds := func() error {
				cmd := exec.Command("kubectl",
					"-n", namespace,
					"get", "valkeycluster",
					"valkeycluster-sample", "-o", "jsonpath={.spec.minReadySeconds}",
				)
				minReadySeconds, err := utils.Run(cmd)
				fmt.Println(string(minReadySeconds))
				ExpectWithOffset(2, err).NotTo(HaveOccurred())
				if string(minReadySeconds) != "15" {
					return fmt.Errorf("expected minReadySeconds value of 15 for valkeycluster but got %s", minReadySeconds)
				}
				return nil
			}
			Eventually(reverifyClusterMinReadySeconds, time.Minute, time.Second).Should(Succeed())

			reverifyStatefulSetMinReadySeconds := func() error {
				cmd := exec.Command("kubectl",
					"-n", namespace,
					"get", "statefulset",
					"-l", fmt.Sprintf("cache/name=%s,app.kubernetes.io/name=valkeyCluster-operator,app.kubernetes.io/managed-by=ValkeyClusterController", "valkeycluster-sample"),
					"-o", "go-template={{ range .items }}"+
						"{{ .spec.minReadySeconds }}"+
						"{{ \"\\n\" }}{{ end }}",
				)
				statefulSetOutput, err := utils.Run(cmd)
				ExpectWithOffset(2, err).NotTo(HaveOccurred())
				statefulSetMinReadyValues := utils.GetNonEmptyLines(string(statefulSetOutput))
				if len(statefulSetMinReadyValues) != 2 {
					return fmt.Errorf("expected 2 statefulsets running, but got %d", len(statefulSetMinReadyValues))
				}
				for _, v := range statefulSetMinReadyValues {
					if v != "15" {
						return fmt.Errorf("expected minReadySeconds value of 15 for statefulset but got %s", v)
					}
				}
				return nil
			}
			Eventually(reverifyStatefulSetMinReadySeconds, time.Minute, time.Second).Should(Succeed())
		})
		It("change storage", func() {
			Skip("local storage provisioner cannot resize storage")
			cmd := exec.Command("kubectl",
				"-n", namespace,
				"patch", "valkeycluster", "valkeycluster-sample",
				"--type=json",
				`-p=[{"op":"replace","path":"/spec/storage/resources/requests/storage","value":"4Gi"}]`,
			)
			_, err := utils.Run(cmd)
			ExpectWithOffset(1, err).NotTo(HaveOccurred())

			verifyPVCResources := func() error {
				cmd = exec.Command("kubectl", "get", "pvc",
					"-l", fmt.Sprintf("cache/name=%s,app.kubernetes.io/name=valkeyCluster-operator,app.kubernetes.io/managed-by=ValkeyClusterController", "valkeycluster-sample"),
					"-o", "go-template={{ range .items }}"+
						"{{ if not .metadata.deletionTimestamp }}"+
						"{{ .metadata.name }}"+
						"{{ \"\\n\" }}{{ end }}{{ end }}",
					"-n", namespace,
				)

				pvcOutput, err := utils.Run(cmd)
				ExpectWithOffset(2, err).NotTo(HaveOccurred())
				pvcNames := utils.GetNonEmptyLines(string(pvcOutput))

				for _, pvcName := range pvcNames {
					cmd = exec.Command("kubectl", "get", "pvc", pvcName,
						"-o", "jsonpath={.spec.resources.requests.storage}",
						"-n", namespace,
					)
					resourceOutput, err := utils.Run(cmd)
					ExpectWithOffset(2, err).NotTo(HaveOccurred())

					if string(resourceOutput) != "4Gi" {
						return fmt.Errorf("expected storage request to be 4Gi for pvc %s but got %s", pvcName, resourceOutput)
					}
				}
				return nil
			}
			EventuallyWithOffset(1, verifyPVCResources, 3*time.Minute, 15*time.Second).Should(Succeed())
		})
		It("apply custom rawConfig", func() {
			customConfig := `port 6379
cluster-enabled yes
cluster-config-file nodes.conf
cluster-node-timeout 5000
appendonly yes
protected-mode no
cluster-preferred-endpoint-type ip
maxmemory 12mb`
			valkeyConfHash := fmt.Sprintf("%x", sha256.Sum256([]byte(customConfig)))
			patchData := fmt.Sprintf(`[{"op": "add", "path": "/spec/valkeyConfig","value":{"rawConfig":%q}}]`, customConfig)
			patchCmd := exec.Command("kubectl",
				"-n", namespace,
				"patch", "valkeycluster", "valkeycluster-sample",
				"--type=json",
				"-p", patchData,
			)
			_, err := utils.Run(patchCmd)
			ExpectWithOffset(1, err).NotTo(HaveOccurred())

			verifyCustomConfig := func() error {
				// Should query a specific map but works for now
				cmd := exec.Command("kubectl", "get", "configmap",
					"-l", fmt.Sprintf("cache/name=%s,app.kubernetes.io/name=valkeyCluster-operator,app.kubernetes.io/managed-by=ValkeyClusterController", "valkeycluster-sample"),
					"-o", "go-template={{ range .items }}"+
						"{{index .data \"valkey.conf\"}}"+
						"{{ end }}",
					"-n", namespace,
				)

				cfgOutput, err := utils.Run(cmd)
				ExpectWithOffset(2, err).NotTo(HaveOccurred())
				if strings.Compare(string(cfgOutput), valkeyConfHash) == 0 {
					return fmt.Errorf("expected configmap to be updated")
				}
				return nil
			}
			Eventually(verifyCustomConfig, time.Minute, time.Second).Should(Succeed())
		})
		It("recover from all pods being deleted", func() {
			cmd := exec.Command("kubectl", "get", "pods",
				"-l", fmt.Sprintf("cache/name=%s,app.kubernetes.io/name=valkeyCluster-operator,app.kubernetes.io/managed-by=ValkeyClusterController", "valkeycluster-sample"),
				"-o", "go-template={{ range .items }}"+
					"{{ if not .metadata.deletionTimestamp }}"+
					"{{ .metadata.name }}"+
					"{{ \"\\n\" }}{{ end }}{{ end }}",
				"-n", namespace,
			)

			podOutput, err := utils.Run(cmd)
			ExpectWithOffset(2, err).NotTo(HaveOccurred())
			podNames := utils.GetNonEmptyLines(string(podOutput))

			// delete all pods
			args := []string{"delete", "pod", "--force", "--grace-period=0",
				"-n", namespace}
			args = append(args, podNames...)
			cmd = exec.Command("kubectl", args...)
			_, err = utils.Run(cmd)
			ExpectWithOffset(2, err).NotTo(HaveOccurred())
			EventuallyWithOffset(1, verifyClusterState("valkeycluster-sample", 2, 2), 10*time.Minute, 15*time.Second).Should(Succeed())
		})
		It("be deleted", func() {
			cmd := exec.Command("kubectl",
				"-n", namespace,
				"delete", "valkeycluster", "valkeycluster-sample",
			)
			_, err := utils.Run(cmd)
			ExpectWithOffset(1, err).NotTo(HaveOccurred())

			verifyResourcesDeleted := func() error {
				cmd = exec.Command("kubectl", "get", "pods",
					"-l", fmt.Sprintf("cache/name=%s,app.kubernetes.io/name=valkeyCluster-operator,app.kubernetes.io/managed-by=ValkeyClusterController", "valkeycluster-sample"),
					"-o", "go-template={{ range .items }}"+
						"{{ if not .metadata.deletionTimestamp }}"+
						"{{ .metadata.name }}"+
						"{{ \"\\n\" }}{{ end }}{{ end }}",
					"-n", namespace,
				)

				podOutput, err := utils.Run(cmd)
				ExpectWithOffset(2, err).NotTo(HaveOccurred())
				podNames := utils.GetNonEmptyLines(string(podOutput))
				if len(podNames) != 0 {
					return fmt.Errorf("expect 0 cache pods running, but got %d", len(podNames))
				}
				return nil
			}
			Eventually(verifyResourcesDeleted, 4*time.Minute, 15*time.Second).Should(Succeed())
		})
	})
})

func verifyClusterState(name string, shards, replicas int) func() error {
	return func() error {
		expectedSlots := valkey.SlotCounts(shards)

		cmd := exec.Command("kubectl", "get",
			"pods", "-l", "cache/name=valkeycluster-sample",
			"-o", "go-template={{ range .items }}"+
				"{{ if not .metadata.deletionTimestamp }}"+
				"{{ .metadata.name }}"+
				"{{ \"\\n\" }}{{ end }}{{ end }}",
			"-n", namespace,
		)
		podOutput, err := utils.Run(cmd)
		if err != nil {
			return fmt.Errorf("recieved error getting pods: %v", err)
		}
		podNames := utils.GetNonEmptyLines(string(podOutput))
		if len(podNames) != shards+shards*replicas {
			return fmt.Errorf("expect %d valkey cluster pods running, but got %d", shards+shards*replicas, len(podNames))
		}

		// Validate pod status
		for _, podName := range podNames {
			cmd = exec.Command("kubectl", "get",
				"pods", podName, "-o", "jsonpath={.status.phase}",
				"-n", namespace,
			)
			status, err := utils.Run(cmd)
			if err != nil {
				return fmt.Errorf("received error getting pod phase: %v", err)
			}
			if string(status) != "Running" {
				return fmt.Errorf("valkey node pod (%s) in %s status", podName, status)
			}
		}

		clusterNodes := make([]*valkey.ClusterNode, 0)
		for _, podName := range podNames {
			cmd = exec.Command("kubectl", "-n", namespace, "exec", podName, "--", "valkey-cli", "cluster", "nodes")
			clusterNodesTxt, err := utils.Run(cmd)
			if err != nil {
				return fmt.Errorf("received error running kubectl exec: %v", err)

			}
			if len(clusterNodesTxt) <= 0 {
				return fmt.Errorf("cluster nodes output should exist but got '%s'", clusterNodesTxt)
			}

			cn, err := valkey.ParseClusterNode(string(clusterNodesTxt))
			if err != nil {
				return fmt.Errorf("received error parsing cluster node: %v", err)
			}
			cn.Pod = podName
			clusterNodes = append(clusterNodes, cn)
		}

		if len(clusterNodes) != shards+shards*replicas {
			return fmt.Errorf("expect %d valkey cluster nodes running, but got %d", shards+shards*replicas, len(clusterNodes))
		}

		for shardIdx := 0; shardIdx < shards; shardIdx++ {
			var primaryNode *valkey.ClusterNode
			var replicaNodes []*valkey.ClusterNode
			for _, cn := range clusterNodes {
				if !strings.HasPrefix(cn.Pod, fmt.Sprintf("%s-%d-", name, shardIdx)) {
					continue
				}

				if cn.IsMaster() {
					primaryNode = cn
				} else {
					replicaNodes = append(replicaNodes, cn)
				}
			}
			if primaryNode == nil {
				return fmt.Errorf("shard %d expect pimary to exist", shardIdx)
			}
			if len(replicaNodes) != replicas {
				return fmt.Errorf("shard %d expect %d replica nodes but got %d", shardIdx, replicas, len(replicaNodes))
			}
			if primaryNode.SlotCount() != expectedSlots[shardIdx] {
				return fmt.Errorf("shard %d expect primary node to have %d slots but got %d", shardIdx, expectedSlots[shardIdx], primaryNode.SlotCount())
			}
			for _, replicaNode := range replicaNodes {
				if replicaNode.MasterNodeID != primaryNode.ID {
					return fmt.Errorf("shard %d expect replica node to %s as master node ID but got %s", shardIdx, primaryNode.ID, replicaNode.MasterNodeID)
				}
			}
			return nil
		}
		return nil
	}
}
