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
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/halter/valkey-cluster-operator/test/utils"
)

const (
	probeClusterName    = "valkeycluster-probe"
	legacyClusterName   = "valkeycluster-legacy"
	slowLoadClusterName = "valkeycluster-slowload"
	legacyImage         = "ghcr.io/halter/valkey:8.0.2"
	valkeyContainer     = "valkey-cluster-node"
	controllerDeploy    = "valkey-cluster-operator-controller-manager"

	startupFailOpenSeconds  = 300
	legacyMeetWindowSeconds = 60
	slowLoadKeys            = 10000
	slowLoadKeyDelayMicros  = 10000
)

type configParameter struct {
	name  string
	value string
}

type probeSpec struct {
	Exec *struct {
		Command []string `json:"command"`
	} `json:"exec"`
	InitialDelaySeconds int32 `json:"initialDelaySeconds"`
	PeriodSeconds       int32 `json:"periodSeconds"`
	TimeoutSeconds      int32 `json:"timeoutSeconds"`
	FailureThreshold    int32 `json:"failureThreshold"`
}

type containerSpec struct {
	StartupProbe   *probeSpec `json:"startupProbe"`
	ReadinessProbe *probeSpec `json:"readinessProbe"`
	LivenessProbe  *probeSpec `json:"livenessProbe"`
}

type containerStatus struct {
	Name         string `json:"name"`
	Ready        bool   `json:"ready"`
	Started      *bool  `json:"started"`
	RestartCount int    `json:"restartCount"`
}

func kubectl(args ...string) ([]byte, error) {
	return utils.Run(exec.Command("kubectl", append([]string{"-n", namespace}, args...)...))
}

func kubectlApply(manifest string) error {
	cmd := exec.Command("kubectl", "-n", namespace, "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	_, err := utils.Run(cmd)
	return err
}

func valkeyClusterManifest(name, image string, shards, replicas int, params ...configParameter) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, `apiVersion: cache.halter.io/v1alpha1
kind: ValkeyCluster
metadata:
  name: %s
spec:
  image: %s
  shards: %d
  replicas: %d
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
`, name, image, shards, replicas)
	if len(params) > 0 {
		sb.WriteString("  valkeyConfig:\n    parameters:\n")
		for _, p := range params {
			fmt.Fprintf(&sb, "    - name: %s\n      value: %q\n", p.name, p.value)
		}
	}
	return sb.String()
}

func livePodNames(selector string) ([]string, error) {
	out, err := kubectl("get", "pods", "-l", selector,
		"-o", "go-template={{ range .items }}"+
			"{{ if not .metadata.deletionTimestamp }}"+
			"{{ .metadata.name }}"+
			"{{ \"\\n\" }}{{ end }}{{ end }}",
	)
	if err != nil {
		return nil, err
	}
	return utils.GetNonEmptyLines(string(out)), nil
}

func clusterPodNames(name string) ([]string, error) {
	return livePodNames("cache/name=" + name)
}

func statefulSetNames(name string, shards int) []string {
	names := make([]string, 0, shards)
	for i := 0; i < shards; i++ {
		names = append(names, fmt.Sprintf("%s-%d", name, i))
	}
	return names
}

func getJSON(target interface{}, resource, name, jsonpath string) error {
	out, err := kubectl("get", resource, name, "-o", "jsonpath="+jsonpath)
	if err != nil {
		return err
	}
	if len(out) == 0 {
		return fmt.Errorf("%s %s: %s returned nothing", resource, name, jsonpath)
	}
	if err := json.Unmarshal(out, target); err != nil {
		return fmt.Errorf("%s %s: parsing %s from %q: %w", resource, name, jsonpath, out, err)
	}
	return nil
}

func statefulSetValkeyContainer(sts string) (containerSpec, error) {
	var spec containerSpec
	err := getJSON(&spec, "statefulset", sts, "{.spec.template.spec.containers[0]}")
	return spec, err
}

func podValkeyContainer(pod string) (containerSpec, error) {
	var spec containerSpec
	err := getJSON(&spec, "pod", pod, "{.spec.containers[0]}")
	return spec, err
}

func podValkeyContainerStatus(pod string) (containerStatus, error) {
	var status containerStatus
	err := getJSON(&status, "pod", pod, fmt.Sprintf(`{.status.containerStatuses[?(@.name=="%s")]}`, valkeyContainer))
	return status, err
}

func podField(pod, jsonpath string) (string, error) {
	out, err := kubectl("get", "pod", pod, "-o", "jsonpath="+jsonpath)
	return strings.TrimSpace(string(out)), err
}

func execInValkey(pod string, command ...string) ([]byte, error) {
	args := append([]string{"exec", pod, "-c", valkeyContainer, "--"}, command...)
	return kubectl(args...)
}

func valkeyInfoField(pod, section, field string) (string, error) {
	out, err := execInValkey(pod, "valkey-cli", "INFO", section)
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(string(out), "\n") {
		if value, ok := strings.CutPrefix(strings.TrimSpace(line), field+":"); ok {
			return value, nil
		}
	}
	return "", fmt.Errorf("%s: %s not in INFO %s: %q", pod, field, section, out)
}

func verifyPodsStartedWithoutRestarts(name string) func() error {
	return func() error {
		pods, err := clusterPodNames(name)
		if err != nil {
			return err
		}
		if len(pods) == 0 {
			return fmt.Errorf("no pods found for %s", name)
		}
		for _, pod := range pods {
			phase, err := podField(pod, "{.status.phase}")
			if err != nil {
				return err
			}
			if phase != "Running" {
				return fmt.Errorf("pod %s is %s", pod, phase)
			}
			status, err := podValkeyContainerStatus(pod)
			if err != nil {
				return err
			}
			if status.Started == nil || !*status.Started {
				return fmt.Errorf("pod %s: %s has not passed its startup probe (%+v)", pod, valkeyContainer, status)
			}
			if !status.Ready {
				return fmt.Errorf("pod %s: %s is not ready (%+v)", pod, valkeyContainer, status)
			}
			if status.RestartCount != 0 {
				return fmt.Errorf("pod %s: %s was restarted %d times by the kubelet", pod, valkeyContainer, status.RestartCount)
			}
		}
		return nil
	}
}

func verifyStartupProbeInterpreterExists(name string) func() error {
	return func() error {
		pods, err := clusterPodNames(name)
		if err != nil {
			return err
		}
		if len(pods) == 0 {
			return fmt.Errorf("no pods found for %s", name)
		}
		for _, pod := range pods {
			spec, err := podValkeyContainer(pod)
			if err != nil {
				return err
			}
			if spec.StartupProbe == nil || spec.StartupProbe.Exec == nil || len(spec.StartupProbe.Exec.Command) == 0 {
				return fmt.Errorf("pod %s has no exec startup probe: %+v", pod, spec.StartupProbe)
			}
			interpreter := spec.StartupProbe.Exec.Command[0]
			if _, err := execInValkey(pod, "test", "-x", interpreter); err != nil {
				return fmt.Errorf("pod %s: startup probe interpreter %s is not executable in the image: %w", pod, interpreter, err)
			}
		}
		return nil
	}
}

func headlessServicePublishesNotReady(name string) func() error {
	return func() error {
		out, err := kubectl("get", "service", name+"-headless", "-o", "jsonpath={.spec.publishNotReadyAddresses}")
		if err != nil {
			return err
		}
		if strings.TrimSpace(string(out)) != "true" {
			return fmt.Errorf("%s-headless does not publish not-ready addresses (%q): peers cannot resolve a node held back by its startup probe", name, out)
		}
		return nil
	}
}

func verifyControllerRunning() error {
	pods, err := livePodNames("control-plane=controller-manager")
	if err != nil {
		return err
	}
	if len(pods) != 1 {
		return fmt.Errorf("expect 1 controller pod, but got %d", len(pods))
	}
	phase, err := podField(pods[0], "{.status.phase}")
	if err != nil {
		return err
	}
	if phase != "Running" {
		return fmt.Errorf("controller pod in %s status", phase)
	}
	return nil
}

var _ = Describe("startup probe", Ordered, ContinueOnFailure, func() {
	BeforeAll(func() {
		By("creating manager namespace")
		cmd := exec.Command("kubectl", "create", "ns", namespace)
		_, _ = utils.Run(cmd)

		By("installing CRDs")
		cmd = exec.Command("make", "install")
		_, err := utils.Run(cmd)
		ExpectWithOffset(1, err).NotTo(HaveOccurred())

		By("deploying the controller-manager")
		cmd = exec.Command("make", "deploy", "IMG=valkey-cluster-operator:latest")
		_, err = utils.Run(cmd)
		ExpectWithOffset(1, err).NotTo(HaveOccurred())

		By("waiting for controller-manager to be ready")
		EventuallyWithOffset(1, verifyControllerRunning, time.Minute, 2*time.Second).Should(Succeed())
	})

	AfterAll(func() {
		By("cleaning up startup probe test resources")
		for _, name := range []string{probeClusterName, legacyClusterName, slowLoadClusterName} {
			_, _ = kubectl("delete", "--timeout=60s", "--ignore-not-found", "valkeycluster", name)
			_, _ = kubectl("delete", "--timeout=30s", "--ignore-not-found", "pvc", "-l", "cache/name="+name)
		}
		_, _ = kubectl("delete", "--timeout=10s", "--ignore-not-found", "deployment", controllerDeploy)
	})

	It("gates readiness on a startup probe that fails open before the kubelet restart budget", func() {
		By("creating a two shard cluster without replicas")
		Expect(kubectlApply(valkeyClusterManifest(probeClusterName, "valkey-server:latest", 2, 0))).To(Succeed())
		Eventually(verifyClusterState(probeClusterName, 2, 0, ""), 4*time.Minute, 15*time.Second).Should(Succeed())

		By("checking the StatefulSet probe configuration")
		for _, sts := range statefulSetNames(probeClusterName, 2) {
			spec, err := statefulSetValkeyContainer(sts)
			Expect(err).NotTo(HaveOccurred())
			Expect(spec.StartupProbe).NotTo(BeNil(), "%s has no startup probe", sts)
			Expect(spec.StartupProbe.Exec).NotTo(BeNil(), "%s startup probe is not an exec probe", sts)
			Expect(spec.StartupProbe.Exec.Command).To(HaveLen(2), "%s startup probe command %v", sts, spec.StartupProbe.Exec.Command)
			Expect(spec.StartupProbe.Exec.Command[1]).To(Equal("/scripts/startup.sh"))
			budget := spec.StartupProbe.FailureThreshold * spec.StartupProbe.PeriodSeconds
			Expect(budget).To(BeNumerically(">", startupFailOpenSeconds),
				"%s: failureThreshold*periodSeconds (%ds) must exceed the script's %ds fail-open timeout or the kubelet restarts the container first",
				sts, budget, startupFailOpenSeconds)
			Expect(spec.ReadinessProbe).NotTo(BeNil())
			Expect(spec.ReadinessProbe.InitialDelaySeconds).To(BeZero(), "%s readiness probe still carries initialDelaySeconds", sts)
		}

		By("checking the ConfigMap ships the startup script")
		out, err := kubectl("get", "configmap", probeClusterName, "-o", `jsonpath={.data.startup\.sh}`)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(out)).To(ContainSubstring("Startup check"))

		By("checking the probe interpreter exists in the image")
		Expect(verifyStartupProbeInterpreterExists(probeClusterName)()).To(Succeed())

		By("checking the headless service publishes pods that are still starting up")
		Expect(headlessServicePublishesNotReady(probeClusterName)()).To(Succeed())
	})

	It("marks every pod started without kubelet restarts and leaves the StatefulSets alone", func() {
		Eventually(verifyPodsStartedWithoutRestarts(probeClusterName), 2*time.Minute, 5*time.Second).Should(Succeed())

		generations := map[string]string{}
		for _, sts := range statefulSetNames(probeClusterName, 2) {
			out, err := kubectl("get", "statefulset", sts, "-o", "jsonpath={.metadata.generation}")
			Expect(err).NotTo(HaveOccurred())
			generations[sts] = string(out)
		}
		Consistently(func() error {
			for sts, generation := range generations {
				out, err := kubectl("get", "statefulset", sts, "-o", "jsonpath={.metadata.generation}")
				if err != nil {
					return err
				}
				if string(out) != generation {
					return fmt.Errorf("%s generation moved from %s to %s while idle: the operator keeps rewriting the pod template", sts, generation, out)
				}
			}
			return nil
		}, 45*time.Second, 5*time.Second).Should(Succeed())
	})

	It("passes the startup script on a healthy node under the image shell", func() {
		pods, err := clusterPodNames(probeClusterName)
		Expect(err).NotTo(HaveOccurred())
		Expect(pods).NotTo(BeEmpty())
		for _, pod := range pods {
			spec, err := podValkeyContainer(pod)
			Expect(err).NotTo(HaveOccurred())
			out, err := execInValkey(pod, spec.StartupProbe.Exec.Command...)
			Expect(err).NotTo(HaveOccurred(), "pod %s: %s", pod, out)
			Expect(string(out)).To(ContainSubstring("Startup check passed"))
		}
	})

	It("re-applies the probe to StatefulSets created by an older operator and rolls their pods", func() {
		By("stopping the operator")
		_, err := kubectl("scale", "deployment", controllerDeploy, "--replicas=0")
		Expect(err).NotTo(HaveOccurred())
		Eventually(func() ([]string, error) {
			return livePodNames("control-plane=controller-manager")
		}, time.Minute, 2*time.Second).Should(BeEmpty())

		By("stripping the probe from the StatefulSets the way an older operator left them")
		for _, sts := range statefulSetNames(probeClusterName, 2) {
			_, err := kubectl("patch", "statefulset", sts, "--type=json",
				`-p=[{"op":"remove","path":"/spec/template/spec/containers/0/startupProbe"},`+
					`{"op":"add","path":"/spec/template/spec/containers/0/readinessProbe/initialDelaySeconds","value":30}]`)
			Expect(err).NotTo(HaveOccurred())
		}

		By("hiding not-ready pods from the headless service the way an older operator left it")
		_, err = kubectl("patch", "service", probeClusterName+"-headless", "--type=merge", `-p={"spec":{"publishNotReadyAddresses":false}}`)
		Expect(err).NotTo(HaveOccurred())

		By("recreating the pods on the probe-less template")
		pods, err := clusterPodNames(probeClusterName)
		Expect(err).NotTo(HaveOccurred())
		Expect(pods).To(HaveLen(2))
		_, err = kubectl(append([]string{"delete", "pod"}, pods...)...)
		Expect(err).NotTo(HaveOccurred())
		Eventually(func() error {
			pods, err := clusterPodNames(probeClusterName)
			if err != nil {
				return err
			}
			if len(pods) != 2 {
				return fmt.Errorf("expected 2 pods, got %d", len(pods))
			}
			for _, pod := range pods {
				spec, err := podValkeyContainer(pod)
				if err != nil {
					return err
				}
				if spec.StartupProbe != nil {
					return fmt.Errorf("pod %s still carries a startup probe", pod)
				}
				phase, err := podField(pod, "{.status.phase}")
				if err != nil {
					return err
				}
				if phase != "Running" {
					return fmt.Errorf("pod %s is %s", pod, phase)
				}
			}
			return nil
		}, 3*time.Minute, 5*time.Second).Should(Succeed())
		oldUIDs := map[string]string{}
		for _, pod := range pods {
			uid, err := podField(pod, "{.metadata.uid}")
			Expect(err).NotTo(HaveOccurred())
			oldUIDs[pod] = uid
		}

		By("starting the operator again")
		_, err = kubectl("scale", "deployment", controllerDeploy, "--replicas=1")
		Expect(err).NotTo(HaveOccurred())
		Eventually(verifyControllerRunning, time.Minute, 2*time.Second).Should(Succeed())

		By("waiting for the operator to restore the probe and roll every pod onto it")
		Eventually(func() error {
			if err := headlessServicePublishesNotReady(probeClusterName)(); err != nil {
				return err
			}
			for _, sts := range statefulSetNames(probeClusterName, 2) {
				spec, err := statefulSetValkeyContainer(sts)
				if err != nil {
					return err
				}
				if spec.StartupProbe == nil {
					return fmt.Errorf("%s: startup probe not restored", sts)
				}
				if spec.ReadinessProbe == nil || spec.ReadinessProbe.InitialDelaySeconds != 0 {
					return fmt.Errorf("%s: readiness probe initialDelaySeconds not cleared: %+v", sts, spec.ReadinessProbe)
				}
				out, err := kubectl("get", "statefulset", sts, "-o", "jsonpath={.status.currentRevision} {.status.updateRevision}")
				if err != nil {
					return err
				}
				revisions := strings.Fields(string(out))
				if len(revisions) != 2 || revisions[0] != revisions[1] {
					return fmt.Errorf("%s: rollout in progress, revisions %q", sts, out)
				}
			}
			pods, err := clusterPodNames(probeClusterName)
			if err != nil {
				return err
			}
			if len(pods) != 2 {
				return fmt.Errorf("expected 2 pods, got %d", len(pods))
			}
			for _, pod := range pods {
				spec, err := podValkeyContainer(pod)
				if err != nil {
					return err
				}
				if spec.StartupProbe == nil {
					return fmt.Errorf("pod %s has not been rolled onto the template with the startup probe", pod)
				}
				uid, err := podField(pod, "{.metadata.uid}")
				if err != nil {
					return err
				}
				if uid == oldUIDs[pod] {
					return fmt.Errorf("pod %s was not replaced by the rolling update", pod)
				}
			}
			return verifyClusterState(probeClusterName, 2, 0, "")()
		}, 6*time.Minute, 15*time.Second).Should(Succeed())
		Eventually(verifyPodsStartedWithoutRestarts(probeClusterName), time.Minute, 2*time.Second).Should(Succeed())
	})

	It("recovers well inside the fail-open window when every pod restarts at once", func() {
		pods, err := clusterPodNames(probeClusterName)
		Expect(err).NotTo(HaveOccurred())
		Expect(pods).To(HaveLen(2))

		By("writing a key")
		_, err = execInValkey(pods[0], "valkey-cli", "-c", "SET", "startup-probe-key", "survives-restart")
		Expect(err).NotTo(HaveOccurred())

		By("deleting every pod")
		deletedAt := time.Now()
		_, err = kubectl(append([]string{"delete", "pod"}, pods...)...)
		Expect(err).NotTo(HaveOccurred())

		By("checking a restarting peer is resolvable through the headless service before it is ready")
		peer := fmt.Sprintf("%s-1-0.%s-headless.%s.svc.cluster.local", probeClusterName, probeClusterName, namespace)
		Eventually(func() error {
			out, err := execInValkey(probeClusterName+"-0-0", "getent", "hosts", peer)
			if err != nil {
				return err
			}
			_, _ = fmt.Fprintf(GinkgoWriter, "%s resolved %.1fs after deletion: %s\n", peer, time.Since(deletedAt).Seconds(), strings.TrimSpace(string(out)))
			return nil
		}, 90*time.Second, 2*time.Second).Should(Succeed())

		By("waiting for the cluster to reform")
		Eventually(verifyClusterState(probeClusterName, 2, 0, ""), 4*time.Minute, 10*time.Second).Should(Succeed())
		_, _ = fmt.Fprintf(GinkgoWriter, "cluster healthy %.0fs after deleting every pod\n", time.Since(deletedAt).Seconds())
		Eventually(verifyPodsStartedWithoutRestarts(probeClusterName), time.Minute, 2*time.Second).Should(Succeed())
		recovery := time.Since(deletedAt)
		_, _ = fmt.Fprintf(GinkgoWriter, "every pod started and ready %.0fs after deleting every pod\n", recovery.Seconds())
		Expect(recovery).To(BeNumerically("<", startupFailOpenSeconds*time.Second),
			"recovery took %.0fs: the nodes only became ready through the %ds fail-open timeout", recovery.Seconds(), startupFailOpenSeconds)

		By("verifying the key survived")
		Eventually(func() error {
			out, err := execInValkey(probeClusterName+"-0-0", "valkey-cli", "-c", "GET", "startup-probe-key")
			if err != nil {
				return err
			}
			if strings.TrimSpace(string(out)) != "survives-restart" {
				return fmt.Errorf("unexpected value %q", strings.TrimSpace(string(out)))
			}
			return nil
		}, time.Minute, 5*time.Second).Should(Succeed())
	})

	It("starts a cluster on the legacy alpine image", func() {
		Expect(kubectlApply(valkeyClusterManifest(legacyClusterName, legacyImage, 1, 0))).To(Succeed())
		Eventually(func() error {
			if err := verifyPodsStartedWithoutRestarts(legacyClusterName)(); err != nil {
				return err
			}
			return verifyClusterState(legacyClusterName, 1, 0, "")()
		}, 4*time.Minute, 10*time.Second).Should(Succeed())
		Expect(verifyStartupProbeInterpreterExists(legacyClusterName)()).To(Succeed())
	})

	It("recovers from a full restart when a node loads its dataset slower than the legacy meet window", func() {
		By("creating a two shard cluster without replicas that loads keys slowly")
		Expect(kubectlApply(valkeyClusterManifest(slowLoadClusterName, "valkey-server:latest", 2, 0,
			configParameter{"key-load-delay", strconv.Itoa(slowLoadKeyDelayMicros)},
			configParameter{"loading-process-events-interval-bytes", "1024"},
		))).To(Succeed())
		Eventually(verifyClusterState(slowLoadClusterName, 2, 0, ""), 4*time.Minute, 15*time.Second).Should(Succeed())
		Eventually(verifyPodsStartedWithoutRestarts(slowLoadClusterName), time.Minute, 2*time.Second).Should(Succeed())
		pods, err := clusterPodNames(slowLoadClusterName)
		Expect(err).NotTo(HaveOccurred())
		Expect(pods).To(HaveLen(2))

		By("writing keys that all hash to one node")
		script := fmt.Sprintf(`i=0; while [ "$i" -lt %d ]; do echo "SET {slow}:$i v"; i=$((i+1)); done | valkey-cli -c >/dev/null`, slowLoadKeys)
		_, err = execInValkey(pods[0], "sh", "-c", script)
		Expect(err).NotTo(HaveOccurred())
		slowPod := ""
		for _, pod := range pods {
			out, err := execInValkey(pod, "valkey-cli", "DBSIZE")
			Expect(err).NotTo(HaveOccurred())
			if strings.TrimSpace(string(out)) == strconv.Itoa(slowLoadKeys) {
				slowPod = pod
			}
		}
		Expect(slowPod).NotTo(BeEmpty(), "no pod holds all %d keys", slowLoadKeys)

		By("rewriting the AOF so the keys load from its RDB preamble")
		_, err = execInValkey(slowPod, "valkey-cli", "BGREWRITEAOF")
		Expect(err).NotTo(HaveOccurred())
		Eventually(func() error {
			for _, field := range []string{"aof_rewrite_scheduled", "aof_rewrite_in_progress"} {
				value, err := valkeyInfoField(slowPod, "persistence", field)
				if err != nil {
					return err
				}
				if value != "0" {
					return fmt.Errorf("%s %s=%s", slowPod, field, value)
				}
			}
			return nil
		}, time.Minute, 2*time.Second).Should(Succeed())

		By("deleting every pod")
		deletedAt := time.Now()
		_, err = kubectl(append([]string{"delete", "pod"}, pods...)...)
		Expect(err).NotTo(HaveOccurred())

		By("waiting for the node to start loading its dataset")
		var loadingSeenAt time.Time
		Eventually(func() error {
			loading, err := valkeyInfoField(slowPod, "persistence", "loading")
			if err != nil {
				return err
			}
			if loading != "1" {
				return fmt.Errorf("%s loading=%s", slowPod, loading)
			}
			loadingSeenAt = time.Now()
			return nil
		}, 2*time.Minute, time.Second).Should(Succeed())

		By("waiting for the dataset to finish loading")
		Eventually(func() (string, error) {
			return valkeyInfoField(slowPod, "persistence", "loading")
		}, 4*time.Minute, time.Second).Should(Equal("0"))
		loadDuration := time.Since(loadingSeenAt)
		_, _ = fmt.Fprintf(GinkgoWriter, "%s loaded its dataset in %.0fs\n", slowPod, loadDuration.Seconds())
		Expect(loadDuration).To(BeNumerically(">", legacyMeetWindowSeconds*time.Second),
			"the dataset loaded in %.0fs, which does not outlast the %ds meet window this test is meant to exceed", loadDuration.Seconds(), legacyMeetWindowSeconds)

		By("waiting for the cluster to reform")
		Eventually(verifyClusterState(slowLoadClusterName, 2, 0, ""), 3*time.Minute, 5*time.Second).Should(Succeed())
		Eventually(verifyPodsStartedWithoutRestarts(slowLoadClusterName), time.Minute, 2*time.Second).Should(Succeed())
		recovery := time.Since(deletedAt)
		_, _ = fmt.Fprintf(GinkgoWriter, "every pod started and ready %.0fs after deleting every pod\n", recovery.Seconds())
		Expect(recovery).To(BeNumerically("<", startupFailOpenSeconds*time.Second),
			"recovery took %.0fs: the nodes only re-met through the %ds fail-open timeout", recovery.Seconds(), startupFailOpenSeconds)

		By("verifying the keys survived")
		out, err := execInValkey(slowPod, "valkey-cli", "DBSIZE")
		Expect(err).NotTo(HaveOccurred())
		Expect(strings.TrimSpace(string(out))).To(Equal(strconv.Itoa(slowLoadKeys)))
	})
})
