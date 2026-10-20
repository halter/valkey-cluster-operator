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
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/halter/valkey-cluster-operator/test/utils"
)

const (
	autoscalingClusterName = "valkeycluster-autoscaling"
	benchPodName           = "valkey-bench"
	valkeyContainer        = "valkey-cluster-node"
)

// autoscalingPod is what the auto-scaling test reads back from each pod.
type autoscalingPod struct {
	name, uid, ip, cpuLimit, memoryLimit string
	restarts                             int32
}

var _ = Describe("resource auto-scaling", Ordered, func() {
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

		By("creating a cluster with CPU and memory auto-scaling")
		cmd = exec.Command("kubectl", "apply", "-n", namespace, "-f", "-")
		cmd.Stdin = strings.NewReader(fmt.Sprintf(`apiVersion: cache.halter.io/v1alpha1
kind: ValkeyCluster
metadata:
  name: %s
spec:
  image: valkey-server:8.0.5
  shards: 1
  replicas: 1
  minReadySeconds: 5
  initialDelaySeconds: 5
  resources:
    requests:
      cpu: 100m
      memory: 256Mi
    limits:
      cpu: 200m
      memory: 512Mi
  storage:
    resources:
      requests:
        storage: 1Gi
    storageClassName: standard
  valkeyConfig:
    parameters:
    - name: enable-debug-command
      value: local
  autoscaling:
    maxResources:
      cpu: "1"
      memory: 1Gi
`, autoscalingClusterName))
		_, err = utils.Run(cmd)
		ExpectWithOffset(1, err).NotTo(HaveOccurred())
		EventuallyWithOffset(1, verifyClusterState(autoscalingClusterName, 1, 1, ""),
			5*time.Minute, 15*time.Second).Should(Succeed())
	})

	AfterAll(func() {
		By("cleaning up auto-scaling test resources")
		cmd := exec.Command("kubectl", "delete", "--grace-period=0", "--force", "--ignore-not-found", "pod", benchPodName, "-n", namespace)
		_, _ = utils.Run(cmd)

		cmd = exec.Command("kubectl", "delete", "--timeout=30s", "valkeycluster", autoscalingClusterName,
			"-n", namespace,
		)
		_, _ = utils.Run(cmd)

		cmd = exec.Command("kubectl", "delete", "--timeout=30s", "pvc",
			"-l", fmt.Sprintf("cache/name=%s", autoscalingClusterName),
			"-n", namespace,
		)
		_, _ = utils.Run(cmd)

		cmd = exec.Command("kubectl", "delete", "--timeout=10s", "deployment", "valkey-cluster-operator-controller-manager",
			"-n", namespace,
		)
		_, _ = utils.Run(cmd)
	})

	It("grows memory in place and shrinks it after usage falls", func() {
		before, err := autoscalingPods()
		Expect(err).NotTo(HaveOccurred())
		primary, err := autoscalingPrimary(before)
		Expect(err).NotTo(HaveOccurred())

		By("filling the primary with about 400MB of data")
		_, err = valkeyCLI(primary.name, "DEBUG", "POPULATE", "400000", "key", "1000")
		Expect(err).NotTo(HaveOccurred())

		By("waiting for memory to grow in place")
		Eventually(func() error {
			size, err := autoscaledSize("memory")
			if err != nil {
				return err
			}
			if size.Cmp(resource.MustParse("512Mi")) <= 0 {
				return fmt.Errorf("status.autoscaledResources.memory is %s, want more than 512Mi", size.String())
			}
			return podsMatch(before, true, true, func(p autoscalingPod) string { return p.memoryLimit }, size)
		}, 3*time.Minute, 10*time.Second).Should(Succeed())

		By("emptying the primary and backdating the last scale past the scale-down delay")
		_, err = valkeyCLI(primary.name, "FLUSHALL")
		Expect(err).NotTo(HaveOccurred())
		_, err = valkeyCLI(primary.name, "MEMORY", "PURGE")
		Expect(err).NotTo(HaveOccurred())
		cmd := exec.Command("kubectl", "patch", "valkeycluster", autoscalingClusterName, "-n", namespace,
			"--subresource=status", "--type=merge",
			"-p", fmt.Sprintf(`{"status":{"lastResourceScaleTime":%q}}`, time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)))
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred())

		By("waiting for memory to shrink back to spec.resources")
		Eventually(func() error {
			size, err := autoscaledSize("memory")
			if err != nil {
				return err
			}
			if size.Cmp(resource.MustParse("512Mi")) != 0 {
				return fmt.Errorf("status.autoscaledResources.memory is %s, want 512Mi", size.String())
			}
			return podsMatch(before, false, false, func(p autoscalingPod) string { return p.memoryLimit }, size)
		}, 6*time.Minute, 10*time.Second).Should(Succeed())
	})

	It("grows CPU in place under load", func() {
		before, err := autoscalingPods()
		Expect(err).NotTo(HaveOccurred())
		primary, err := autoscalingPrimary(before)
		Expect(err).NotTo(HaveOccurred())

		By("running valkey-benchmark against the primary")
		cmd := exec.Command("kubectl", "run", benchPodName, "-n", namespace,
			"--image=valkey-server:8.0.5", "--image-pull-policy=IfNotPresent", "--restart=Never", "--command", "--",
			"valkey-benchmark", "-h", primary.ip, "-p", "6379", "-n", "100000000", "-c", "50", "-P", "16", "-t", "set,get", "-q")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred())

		By("waiting for CPU to grow in place")
		Eventually(func() error {
			size, err := autoscaledSize("cpu")
			if err != nil {
				return err
			}
			if size.Cmp(resource.MustParse("200m")) <= 0 {
				return fmt.Errorf("status.autoscaledResources.cpu is %s, want more than 200m", size.String())
			}
			return podsMatch(before, true, false, func(p autoscalingPod) string { return p.cpuLimit }, size)
		}, 5*time.Minute, 15*time.Second).Should(Succeed())

		cmd = exec.Command("kubectl", "delete", "--grace-period=0", "--force", "pod", benchPodName, "-n", namespace)
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred())
	})
})

// podsMatch checks that every pod's valkey container limit equals size and,
// when asked, that the pods are the ones in before with the same container
// restart counts.
func podsMatch(before []autoscalingPod, sameUIDs, sameRestarts bool,
	limit func(autoscalingPod) string, size resource.Quantity) error {
	pods, err := autoscalingPods()
	if err != nil {
		return err
	}
	if len(pods) != len(before) {
		return fmt.Errorf("expected %d pods, got %d", len(before), len(pods))
	}
	previous := map[string]autoscalingPod{}
	for _, p := range before {
		previous[p.name] = p
	}
	for _, p := range pods {
		got, err := resource.ParseQuantity(limit(p))
		if err != nil || got.Cmp(size) != 0 {
			return fmt.Errorf("pod %s has limit %q, want %s", p.name, limit(p), size.String())
		}
		if sameUIDs && p.uid != previous[p.name].uid {
			return fmt.Errorf("pod %s was replaced, want it resized in place", p.name)
		}
		if sameRestarts && p.restarts != previous[p.name].restarts {
			return fmt.Errorf("pod %s restarted its valkey container %d times, want %d",
				p.name, p.restarts, previous[p.name].restarts)
		}
	}
	return nil
}

// autoscaledSize pokes the cluster so the operator reconciles without waiting
// for its periodic requeue, then reads status.autoscaledResources[name].
func autoscaledSize(name string) (resource.Quantity, error) {
	cmd := exec.Command("kubectl", "annotate", "--overwrite", "valkeycluster", autoscalingClusterName, "-n", namespace,
		fmt.Sprintf("e2e/poke=%d", time.Now().UnixNano()))
	if _, err := utils.Run(cmd); err != nil {
		return resource.Quantity{}, err
	}
	cmd = exec.Command("kubectl", "get", "valkeycluster", autoscalingClusterName, "-n", namespace,
		"-o", fmt.Sprintf("jsonpath={.status.autoscaledResources.%s}", name))
	out, _, err := utils.RunWithSplitOutput(cmd)
	if err != nil {
		return resource.Quantity{}, err
	}
	return resource.ParseQuantity(strings.TrimSpace(string(out)))
}

func autoscalingPods() ([]autoscalingPod, error) {
	cmd := exec.Command("kubectl", "get", "pods", "-n", namespace,
		"-l", fmt.Sprintf("cache/name=%s", autoscalingClusterName), "-o", "json")
	out, _, err := utils.RunWithSplitOutput(cmd)
	if err != nil {
		return nil, err
	}
	var list struct {
		Items []struct {
			Metadata struct {
				Name              string  `json:"name"`
				UID               string  `json:"uid"`
				DeletionTimestamp *string `json:"deletionTimestamp"`
			} `json:"metadata"`
			Spec struct {
				Containers []struct {
					Name      string `json:"name"`
					Resources struct {
						Limits map[string]string `json:"limits"`
					} `json:"resources"`
				} `json:"containers"`
			} `json:"spec"`
			Status struct {
				PodIP             string `json:"podIP"`
				ContainerStatuses []struct {
					Name         string `json:"name"`
					RestartCount int32  `json:"restartCount"`
				} `json:"containerStatuses"`
			} `json:"status"`
		} `json:"items"`
	}
	if err := json.Unmarshal(out, &list); err != nil {
		return nil, err
	}
	var pods []autoscalingPod
	for _, item := range list.Items {
		if item.Metadata.DeletionTimestamp != nil {
			continue
		}
		p := autoscalingPod{name: item.Metadata.Name, uid: item.Metadata.UID, ip: item.Status.PodIP}
		for _, c := range item.Spec.Containers {
			if c.Name == valkeyContainer {
				p.cpuLimit, p.memoryLimit = c.Resources.Limits["cpu"], c.Resources.Limits["memory"]
			}
		}
		for _, c := range item.Status.ContainerStatuses {
			if c.Name == valkeyContainer {
				p.restarts = c.RestartCount
			}
		}
		pods = append(pods, p)
	}
	return pods, nil
}

func autoscalingPrimary(pods []autoscalingPod) (autoscalingPod, error) {
	for _, p := range pods {
		out, err := valkeyCLI(p.name, "INFO", "replication")
		if err != nil {
			return autoscalingPod{}, err
		}
		if strings.Contains(out, "role:master") {
			return p, nil
		}
	}
	return autoscalingPod{}, fmt.Errorf("no primary among %d pods", len(pods))
}

func valkeyCLI(podName string, args ...string) (string, error) {
	cmd := exec.Command("kubectl", append([]string{
		"-n", namespace, "exec", podName, "-c", valkeyContainer, "--", "valkey-cli"}, args...)...)
	out, err := utils.Run(cmd)
	return string(out), err
}
