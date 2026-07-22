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
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/halter/valkey-cluster-operator/test/utils"
)

// The declarative-config tests need a cluster whose image tag positively
// parses as Valkey >= 8.0: the operator-managed replication defaults are
// version-gated, and the sample cluster used by the "controller" Describe
// runs the unversioned tag "latest", which deliberately gets no defaults.
const (
	configClusterName = "valkeycluster-config"
	// configClusterMemory is the pod memory limit of the test cluster; the
	// operator derives its replication-tuning defaults from this value.
	configClusterMemory      = "314Mi"
	configClusterMemoryBytes = int64(314) * 1024 * 1024
	configClusterShards      = 2
	configClusterReplicas    = 1
)

var _ = Describe("valkey config", Ordered, func() {
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
		By("cleaning up config test resources")
		cmd := exec.Command("kubectl", "delete", "--timeout=30s", "valkeycluster", configClusterName,
			"-n", namespace,
		)
		_, _ = utils.Run(cmd)

		cmd = exec.Command("kubectl", "delete", "--timeout=30s", "pvc",
			"-l", fmt.Sprintf("cache/name=%s", configClusterName),
			"-n", namespace,
		)
		_, _ = utils.Run(cmd)

		cmd = exec.Command("kubectl", "delete", "--timeout=10s", "deployment", "valkey-cluster-operator-controller-manager",
			"-n", namespace,
		)
		_, _ = utils.Run(cmd)
	})

	It("should apply operator-managed defaults to a new cluster", func() {
		By("creating a ValkeyCluster with a versioned >= 8.0 image")
		cmd := exec.Command("kubectl", "apply", "-n", namespace, "-f", "-")
		cmd.Stdin = strings.NewReader(fmt.Sprintf(`apiVersion: cache.halter.io/v1alpha1
kind: ValkeyCluster
metadata:
  name: %s
spec:
  image: valkey-server:8.0.5
  shards: %d
  replicas: %d
  minReadySeconds: 5
  resources:
    limits:
      cpu: "0.4"
      memory: %s
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
`, configClusterName, configClusterShards, configClusterReplicas, configClusterMemory))
		_, err := utils.Run(cmd)
		ExpectWithOffset(1, err).NotTo(HaveOccurred())

		By("waiting for the cluster to be ready")
		EventuallyWithOffset(1, verifyClusterState(configClusterName, configClusterShards, configClusterReplicas, ""), 3*time.Minute, 15*time.Second).Should(Succeed())

		// Expected values mirror the derivation in the README's
		// "Operator-managed defaults" table for a 314Mi memory limit.
		backlog, hard, soft := expectedManagedDefaults(configClusterMemoryBytes)

		By("verifying the defaults are rendered into the config file")
		verifyConfigMap := func() error {
			return configMapContains(
				fmt.Sprintf("repl-backlog-size %d", backlog),
				fmt.Sprintf("client-output-buffer-limit replica %d %d 120", hard, soft),
				"dual-channel-replication-enabled yes",
			)
		}
		EventuallyWithOffset(1, verifyConfigMap, time.Minute, 5*time.Second).Should(Succeed())

		By("verifying every pod is running with the defaults")
		verifyDefaults := func() error {
			pods, err := configClusterPodNames()
			if err != nil {
				return err
			}
			for _, pod := range pods {
				if err := podConfigEquals(pod, "dual-channel-replication-enabled", "yes"); err != nil {
					return err
				}
				if err := podConfigEquals(pod, "repl-backlog-size", strconv.FormatInt(backlog, 10)); err != nil {
					return err
				}
				// CONFIG GET reports the replica class as "slave" inside the
				// canonical three-class string.
				value, err := podConfigGet(pod, "client-output-buffer-limit")
				if err != nil {
					return err
				}
				expected := fmt.Sprintf("slave %d %d 120", hard, soft)
				if !strings.Contains(value, expected) {
					return fmt.Errorf("pod %s expected client-output-buffer-limit to contain %q but got %q", pod, expected, value)
				}
			}
			return nil
		}
		EventuallyWithOffset(1, verifyDefaults, 2*time.Minute, 5*time.Second).Should(Succeed())
	})

	It("should live-apply spec parameters to running pods without restarts", func() {
		identitiesBefore, err := configClusterPodIdentities()
		ExpectWithOffset(1, err).NotTo(HaveOccurred())

		By("adding runtime-settable parameters to spec.valkeyConfig")
		patchConfigParameters(`[{"name":"maxmemory","value":"32mb"},{"name":"maxmemory-policy","value":"allkeys-lru"}]`)

		By("verifying every pod converges to the new values")
		verifyApplied := func() error {
			pods, err := configClusterPodNames()
			if err != nil {
				return err
			}
			for _, pod := range pods {
				// CONFIG GET canonicalises memory values to byte counts.
				if err := podConfigEquals(pod, "maxmemory", "33554432"); err != nil {
					return err
				}
				if err := podConfigEquals(pod, "maxmemory-policy", "allkeys-lru"); err != nil {
					return err
				}
			}
			return nil
		}
		EventuallyWithOffset(1, verifyApplied, 2*time.Minute, 5*time.Second).Should(Succeed())

		By("verifying no pod was restarted to apply them")
		identitiesAfter, err := configClusterPodIdentities()
		ExpectWithOffset(1, err).NotTo(HaveOccurred())
		ExpectWithOffset(1, identitiesAfter).To(Equal(identitiesBefore))
	})

	It("should converge manual CONFIG SET drift back to spec", func() {
		driftedPod := configClusterName + "-0-0"

		By("manually drifting a managed directive on one pod")
		cmd := exec.Command("kubectl", "-n", namespace, "exec", driftedPod, "-c", "valkey-cluster-node", "--",
			"valkey-cli", "CONFIG", "SET", "maxmemory-policy", "noeviction")
		_, err := utils.Run(cmd)
		ExpectWithOffset(1, err).NotTo(HaveOccurred())
		ExpectWithOffset(1, podConfigEquals(driftedPod, "maxmemory-policy", "noeviction")).To(Succeed())

		By("waiting for the operator to converge the directive back to spec")
		// No spec change accompanies the drift, so nothing is guaranteed to
		// trigger a watch event; convergence is bounded by the operator's
		// periodic requeue (5 minutes on a stable cluster). The generous
		// timeout makes this an honest test of autonomous convergence — the
		// SP-1527 failure mode was precisely that nothing converged manual
		// CONFIG SETs back.
		verifyConverged := func() error {
			return podConfigEquals(driftedPod, "maxmemory-policy", "allkeys-lru")
		}
		EventuallyWithOffset(1, verifyConverged, 7*time.Minute, 15*time.Second).Should(Succeed())
	})

	It("should let spec parameters override operator-managed defaults", func() {
		identitiesBefore, err := configClusterPodIdentities()
		ExpectWithOffset(1, err).NotTo(HaveOccurred())

		By("setting repl-backlog-size in spec to override the derived default")
		patchConfigParameters(`[{"name":"maxmemory","value":"32mb"},{"name":"maxmemory-policy","value":"allkeys-lru"},{"name":"repl-backlog-size","value":"16mb"}]`)

		By("verifying the spec value wins over the default on every pod")
		verifyOverride := func() error {
			pods, err := configClusterPodNames()
			if err != nil {
				return err
			}
			for _, pod := range pods {
				if err := podConfigEquals(pod, "repl-backlog-size", "16777216"); err != nil {
					return err
				}
			}
			return nil
		}
		EventuallyWithOffset(1, verifyOverride, 2*time.Minute, 5*time.Second).Should(Succeed())

		By("verifying the override is stable, not oscillating with the default")
		// A directive present in both the operator defaults and the spec must
		// settle on the spec value: comparing raw entries against a stale
		// config snapshot would re-apply the default and the override on
		// alternating reconciles.
		ConsistentlyWithOffset(1, verifyOverride, 45*time.Second, 5*time.Second).Should(Succeed())

		By("verifying no pod was restarted")
		identitiesAfter, err := configClusterPodIdentities()
		ExpectWithOffset(1, err).NotTo(HaveOccurred())
		ExpectWithOffset(1, identitiesAfter).To(Equal(identitiesBefore))
	})

	It("should surface rejected values without restarting pods", func() {
		identitiesBefore, err := configClusterPodIdentities()
		ExpectWithOffset(1, err).NotTo(HaveOccurred())

		By("setting a value the server rejects at runtime")
		patchConfigParameters(`[{"name":"maxmemory","value":"32mb"},{"name":"maxmemory-policy","value":"bogus-policy"},{"name":"repl-backlog-size","value":"16mb"}]`)

		By("waiting for the ConfigValueRejected warning event")
		verifyEvent := func() error {
			return eventWithReasonExists("ConfigValueRejected")
		}
		EventuallyWithOffset(1, verifyEvent, 2*time.Minute, 5*time.Second).Should(Succeed())

		By("verifying pods keep running with their last good value instead of restarting")
		// A restart here would trade a running-but-stale pod for one that
		// crash-loops on the invalid config file directive.
		verifyNoRestart := func() error {
			identities, err := configClusterPodIdentities()
			if err != nil {
				return err
			}
			for pod, identity := range identitiesBefore {
				if identities[pod] != identity {
					return fmt.Errorf("pod %s was restarted (identity %q -> %q)", pod, identity, identities[pod])
				}
				if err := podConfigEquals(pod, "maxmemory-policy", "allkeys-lru"); err != nil {
					return err
				}
			}
			return nil
		}
		ConsistentlyWithOffset(1, verifyNoRestart, 30*time.Second, 5*time.Second).Should(Succeed())

		By("restoring a valid value and verifying the config file is cleaned up")
		patchConfigParameters(`[{"name":"maxmemory","value":"32mb"},{"name":"maxmemory-policy","value":"allkeys-lru"},{"name":"repl-backlog-size","value":"16mb"}]`)
		verifyCleanConfigMap := func() error {
			if err := configMapContains("maxmemory-policy allkeys-lru"); err != nil {
				return err
			}
			cfg, err := configMapValkeyConf()
			if err != nil {
				return err
			}
			if strings.Contains(cfg, "bogus-policy") {
				return fmt.Errorf("expected configmap to no longer contain the rejected value, got: %s", cfg)
			}
			return nil
		}
		EventuallyWithOffset(1, verifyCleanConfigMap, time.Minute, 5*time.Second).Should(Succeed())
	})

	It("should apply immutable directives via a health-gated rolling restart", func() {
		uidsBefore, err := configClusterPodIdentities()
		ExpectWithOffset(1, err).NotTo(HaveOccurred())

		By("adding an immutable directive to spec.valkeyConfig")
		// io-threads is IMMUTABLE_CONFIG: CONFIG SET rejects it with "can't
		// set immutable config", so the only way to apply it is restarting
		// pods onto the updated config file.
		patchConfigParameters(`[{"name":"maxmemory","value":"32mb"},{"name":"maxmemory-policy","value":"allkeys-lru"},{"name":"repl-backlog-size","value":"16mb"},{"name":"io-threads","value":"2"}]`)

		// The ConfigRequiresRestart warning event is deliberately not
		// asserted here: it is re-emitted on every reconcile of a rollout, so
		// client-go's per-object spam filter (burst of 25, refilling one per
		// 5 minutes) makes its visibility at any given moment best-effort.
		// The warning-event surfacing path is covered by the
		// ConfigValueRejected assertion earlier in this suite; this spec
		// asserts the restart behaviour itself.
		By("waiting for every pod to be replaced and running with the directive")
		expectedPods := configClusterShards + configClusterShards*configClusterReplicas
		verifyRestarted := func() error {
			identities, err := configClusterPodIdentities()
			if err != nil {
				return err
			}
			if len(identities) != expectedPods {
				return fmt.Errorf("expected %d pods but got %d", expectedPods, len(identities))
			}
			for pod, identity := range identities {
				if uidsBefore[pod] == identity {
					return fmt.Errorf("pod %s has not been restarted yet", pod)
				}
				if err := podConfigEquals(pod, "io-threads", "2"); err != nil {
					return err
				}
			}
			// The restarts are gated on full cluster health, so the cluster
			// must come out of the rollout fully converged.
			return verifyClusterState(configClusterName, configClusterShards, configClusterReplicas, "")()
		}
		EventuallyWithOffset(1, verifyRestarted, 10*time.Minute, 15*time.Second).Should(Succeed())
	})
})

// expectedManagedDefaults mirrors the derivation in the README's
// "Operator-managed defaults" table: repl-backlog-size clamp(memory/16,
// 10MiB, 512MiB) and a replica client-output-buffer-limit of clamp(memory/2,
// 64MiB, 4GiB) hard / hard/2 soft.
func expectedManagedDefaults(memoryLimitBytes int64) (backlog, hard, soft int64) {
	clamp := func(v, lo, hi int64) int64 {
		return min(max(v, lo), hi)
	}
	backlog = clamp(memoryLimitBytes/16, 10*1024*1024, 512*1024*1024)
	hard = clamp(memoryLimitBytes/2, 64*1024*1024, 4*1024*1024*1024)
	soft = hard / 2
	return backlog, hard, soft
}

// patchConfigParameters sets spec.valkeyConfig.parameters of the config test
// cluster to the given JSON array.
func patchConfigParameters(parametersJSON string) {
	cmd := exec.Command("kubectl",
		"-n", namespace,
		"patch", "valkeycluster", configClusterName,
		"--type=json",
		fmt.Sprintf(`-p=[{"op":"add","path":"/spec/valkeyConfig","value":{"parameters":%s}}]`, parametersJSON),
	)
	_, err := utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
}

// configClusterPodNames returns the non-terminating pod names of the config
// test cluster.
func configClusterPodNames() ([]string, error) {
	cmd := exec.Command("kubectl", "get",
		"pods", "-l", fmt.Sprintf("cache/name=%s", configClusterName),
		"-o", "go-template={{ range .items }}"+
			"{{ if not .metadata.deletionTimestamp }}"+
			"{{ .metadata.name }}"+
			"{{ \"\\n\" }}{{ end }}{{ end }}",
		"-n", namespace,
	)
	podOutput, err := utils.Run(cmd)
	if err != nil {
		return nil, fmt.Errorf("received error getting pods: %w", err)
	}
	podNames := utils.GetNonEmptyLines(string(podOutput))
	expectedPods := configClusterShards + configClusterShards*configClusterReplicas
	if len(podNames) != expectedPods {
		return nil, fmt.Errorf("expected %d pods but got %d", expectedPods, len(podNames))
	}
	return podNames, nil
}

// configClusterPodIdentities returns pod name -> "<uid> <restartCounts>" for
// the config test cluster, capturing both pod replacement (deletion and
// recreation changes the UID) and in-place container restarts.
func configClusterPodIdentities() (map[string]string, error) {
	cmd := exec.Command("kubectl", "get",
		"pods", "-l", fmt.Sprintf("cache/name=%s", configClusterName),
		"-o", "go-template={{ range .items }}"+
			"{{ if not .metadata.deletionTimestamp }}"+
			"{{ .metadata.name }} {{ .metadata.uid }}"+
			"{{ range .status.containerStatuses }} {{ .restartCount }}{{ end }}"+
			"{{ \"\\n\" }}{{ end }}{{ end }}",
		"-n", namespace,
	)
	podOutput, err := utils.Run(cmd)
	if err != nil {
		return nil, fmt.Errorf("received error getting pod identities: %w", err)
	}
	identities := make(map[string]string)
	for _, line := range utils.GetNonEmptyLines(string(podOutput)) {
		name, identity, found := strings.Cut(strings.TrimSpace(line), " ")
		if !found {
			return nil, fmt.Errorf("unexpected pod identity line %q", line)
		}
		identities[name] = identity
	}
	return identities, nil
}

// podConfigGet returns the live value of a config directive on a pod.
func podConfigGet(podName, parameter string) (string, error) {
	cmd := exec.Command("kubectl", "-n", namespace, "exec", podName, "-c", "valkey-cluster-node", "--",
		"valkey-cli", "CONFIG", "GET", parameter)
	stdout, _, err := utils.RunWithSplitOutput(cmd)
	if err != nil {
		return "", fmt.Errorf("received error running CONFIG GET %s on pod %s: %w", parameter, podName, err)
	}
	lines := utils.GetNonEmptyLines(string(stdout))
	if len(lines) < 2 {
		return "", fmt.Errorf("expected CONFIG GET %s on pod %s to return a value but got %q", parameter, podName, stdout)
	}
	return strings.TrimSpace(lines[1]), nil
}

func podConfigEquals(podName, parameter, expected string) error {
	value, err := podConfigGet(podName, parameter)
	if err != nil {
		return err
	}
	if value != expected {
		return fmt.Errorf("pod %s expected %s to be %q but got %q", podName, parameter, expected, value)
	}
	return nil
}

// configMapValkeyConf returns the rendered valkey.conf of the config test
// cluster.
func configMapValkeyConf() (string, error) {
	cmd := exec.Command("kubectl", "get", "configmap",
		configClusterName,
		"-o", `jsonpath={.data.valkey\.conf}`,
		"-n", namespace,
	)
	cfgOutput, err := utils.Run(cmd)
	if err != nil {
		return "", fmt.Errorf("received error getting configmap: %w", err)
	}
	return string(cfgOutput), nil
}

func configMapContains(expected ...string) error {
	cfg, err := configMapValkeyConf()
	if err != nil {
		return err
	}
	for _, want := range expected {
		if !strings.Contains(cfg, want) {
			return fmt.Errorf("expected configmap to contain %q but got: %s", want, cfg)
		}
	}
	return nil
}

// eventWithReasonExists reports whether the config test cluster has emitted
// an event with the given reason.
func eventWithReasonExists(reason string) error {
	cmd := exec.Command("kubectl", "get", "events",
		"-n", namespace,
		"--field-selector", fmt.Sprintf("reason=%s,involvedObject.name=%s", reason, configClusterName),
		"-o", "go-template={{ range .items }}{{ .reason }}{{ \"\\n\" }}{{ end }}",
	)
	output, err := utils.Run(cmd)
	if err != nil {
		return fmt.Errorf("received error getting events: %w", err)
	}
	if len(utils.GetNonEmptyLines(string(output))) == 0 {
		return fmt.Errorf("expected an event with reason %s for %s", reason, configClusterName)
	}
	return nil
}
