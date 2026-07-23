package controller

import (
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	cachev1alpha1 "github.com/halter/valkey-cluster-operator/api/v1alpha1"
)

// Canonical CONFIG GET outputs in these tests were captured from a live
// ghcr.io/halter/valkey-server:8.0.5 container: memory values come back as
// plain byte counts, client-output-buffer-limit comes back as the full
// three-class string using the "slave" spelling.
var _ = Describe("valkeyConfigValueEqual", func() {
	It("matches identical strings", func() {
		Expect(valkeyConfigValueEqual("maxmemory-policy", "allkeys-lru", "allkeys-lru")).To(BeTrue())
	})

	It("matches case-insensitively", func() {
		Expect(valkeyConfigValueEqual("dual-channel-replication-enabled", "yes", "YES")).To(BeTrue())
	})

	It("does not match different booleans", func() {
		Expect(valkeyConfigValueEqual("dual-channel-replication-enabled", "yes", "no")).To(BeFalse())
	})

	It("matches a suffixed memory value against its canonical byte count", func() {
		Expect(valkeyConfigValueEqual("repl-backlog-size", "512mb", "536870912")).To(BeTrue())
		Expect(valkeyConfigValueEqual("repl-backlog-size", "2gb", "2147483648")).To(BeTrue())
	})

	It("does not match different memory values", func() {
		Expect(valkeyConfigValueEqual("repl-backlog-size", "512mb", "536870913")).To(BeFalse())
		Expect(valkeyConfigValueEqual("repl-backlog-size", "10mb", "536870912")).To(BeFalse())
	})

	It("matches client-output-buffer-limit for the classes the desired value names", func() {
		canonical := "normal 0 0 0 slave 2147483648 1073741824 120 pubsub 33554432 8388608 60"
		Expect(valkeyConfigValueEqual("client-output-buffer-limit",
			"replica 2147483648 1073741824 120", canonical)).To(BeTrue())
		Expect(valkeyConfigValueEqual("client-output-buffer-limit",
			"slave 2gb 1gb 120", canonical)).To(BeTrue())
		Expect(valkeyConfigValueEqual("client-output-buffer-limit",
			"normal 0 0 0 pubsub 32mb 8mb 60", canonical)).To(BeTrue())
	})

	It("does not match client-output-buffer-limit when a named class differs", func() {
		canonical := "normal 0 0 0 slave 268435456 67108864 60 pubsub 33554432 8388608 60"
		Expect(valkeyConfigValueEqual("client-output-buffer-limit",
			"replica 2147483648 1073741824 120", canonical)).To(BeFalse())
	})

	It("does not match malformed client-output-buffer-limit values", func() {
		Expect(valkeyConfigValueEqual("client-output-buffer-limit",
			"replica banana 1073741824 120", "normal 0 0 0 slave 0 0 0 pubsub 0 0 0")).To(BeFalse())
		Expect(valkeyConfigValueEqual("client-output-buffer-limit",
			"replica 1 2", "normal 0 0 0 slave 0 0 0 pubsub 0 0 0")).To(BeFalse())
	})

	It("falls back to inequality for values that are neither identical nor parseable", func() {
		Expect(valkeyConfigValueEqual("appendfilename", "foo.aof", "bar.aof")).To(BeFalse())
	})
})

var _ = Describe("parseValkeyMemory", func() {
	It("parses valkey.conf unit notation", func() {
		type row struct {
			in   string
			want int64
		}
		for _, r := range []row{
			{"1024", 1024},
			{"1k", 1000},
			{"1kb", 1024},
			{"1m", 1000 * 1000},
			{"1mb", 1024 * 1024},
			{"1g", 1000 * 1000 * 1000},
			{"1gb", 1024 * 1024 * 1024},
			{"1GB", 1024 * 1024 * 1024},
			{" 512mb ", 512 * 1024 * 1024},
		} {
			got, ok := parseValkeyMemory(r.in)
			Expect(ok).To(BeTrue(), "input %q", r.in)
			Expect(got).To(Equal(r.want), "input %q", r.in)
		}
	})

	It("rejects non-numeric values", func() {
		for _, in := range []string{"", "banana", "12x", "mb", "1.5gb"} {
			_, ok := parseValkeyMemory(in)
			Expect(ok).To(BeFalse(), "input %q", in)
		}
	})
})

var _ = Describe("imageSupportsManagedDefaults", func() {
	It("accepts the fleet's valkey >= 8.0 images", func() {
		for _, img := range []string{
			"ghcr.io/halter/valkey-server:8.0.5",
			"ghcr.io/halter/valkey-server:8.1.9",
			"ghcr.io/halter/valkey-server:9.1.1",
			"ghcr.io/halter/valkey:8.0.2",
			"localhost:5000/valkey:8.0.5",
			"valkey/valkey:v8.0.5",
		} {
			Expect(imageSupportsManagedDefaults(img)).To(BeTrue(), "image %q", img)
		}
	})

	It("rejects pre-8.0, untagged, digest-pinned, and unparseable images", func() {
		for _, img := range []string{
			"ghcr.io/halter/valkey-server:7.2.5",
			"ghcr.io/halter/valkey-server",
			"ghcr.io/halter/valkey-server@sha256:deadbeef",
			"ghcr.io/halter/valkey-server:latest",
			"localhost:5000/valkey-server",
			"",
		} {
			Expect(imageSupportsManagedDefaults(img)).To(BeFalse(), "image %q", img)
		}
	})
})

var _ = Describe("managedDefaultParameters", func() {
	cluster := func(image, memory, rawConfig string) *cachev1alpha1.ValkeyCluster {
		vc := &cachev1alpha1.ValkeyCluster{
			Spec: cachev1alpha1.ValkeyClusterSpec{
				Image: image,
			},
		}
		if memory != "" {
			vc.Spec.Resources = &corev1.ResourceRequirements{
				Limits: corev1.ResourceList{
					corev1.ResourceMemory: resource.MustParse(memory),
				},
			}
		}
		if rawConfig != "" {
			vc.Spec.ValkeyConfig = &cachev1alpha1.ValkeyConfig{RawConfig: rawConfig}
		}
		return vc
	}

	names := func(params []cachev1alpha1.ValkeyConfigParameter) []string {
		out := make([]string, 0, len(params))
		for _, p := range params {
			out = append(out, p.Name)
		}
		return out
	}

	valueOf := func(params []cachev1alpha1.ValkeyConfigParameter, name string) string {
		for _, p := range params {
			if p.Name == name {
				return p.Value
			}
		}
		return ""
	}

	It("returns nothing when rawConfig is set (expert mode)", func() {
		Expect(managedDefaultParameters(cluster("ghcr.io/halter/valkey-server:8.0.5", "16000Mi", "port 6379"))).To(BeEmpty())
	})

	It("returns nothing for images that do not positively parse as valkey >= 8.0", func() {
		Expect(managedDefaultParameters(cluster("ghcr.io/halter/valkey-server:7.2.5", "16000Mi", ""))).To(BeEmpty())
		Expect(managedDefaultParameters(cluster("", "16000Mi", ""))).To(BeEmpty())
	})

	It("derives the sized directives from the pod memory limit", func() {
		// 16000Mi mirrors halter-device-service-vk-cluster prod
		// (terraform/params/prod.tfvars valkey_memory = 16000).
		params := managedDefaultParameters(cluster("ghcr.io/halter/valkey-server:8.1.9", "16000Mi", ""))
		Expect(names(params)).To(Equal([]string{
			"dual-channel-replication-enabled",
			"repl-backlog-size",
			"client-output-buffer-limit",
		}))
		Expect(valueOf(params, "dual-channel-replication-enabled")).To(Equal("yes"))
		// 16000Mi/16 = 1000Mi, clamped down to the 512MiB max — the SP-1527
		// incident remediation value.
		Expect(valueOf(params, "repl-backlog-size")).To(Equal("536870912"))
		// 16000Mi/2 = 8000Mi, clamped down to the 4GiB max; soft = hard/2.
		Expect(valueOf(params, "client-output-buffer-limit")).To(Equal("replica 4294967296 2147483648 120"))
	})

	It("clamps the sized directives up to their minimums on small pods", func() {
		params := managedDefaultParameters(cluster("ghcr.io/halter/valkey-server:8.0.5", "128Mi", ""))
		// 128Mi/16 = 8Mi -> min 10MiB (the compiled default); 128Mi/2 = 64Mi
		// hits the 64MiB hard-limit floor exactly.
		Expect(valueOf(params, "repl-backlog-size")).To(Equal("10485760"))
		Expect(valueOf(params, "client-output-buffer-limit")).To(Equal("replica 67108864 33554432 120"))
	})

	It("keeps backlog no larger than the replica hard limit across sizes", func() {
		// A replica hard limit below repl-backlog-size is ignored by valkey
		// (valkey.conf 8.0.5), so the derivation must never produce that.
		for _, mem := range []string{"64Mi", "128Mi", "1Gi", "2Gi", "8Gi", "16000Mi", "64Gi"} {
			params := managedDefaultParameters(cluster("ghcr.io/halter/valkey-server:8.0.5", mem, ""))
			backlog, ok := parseValkeyMemory(valueOf(params, "repl-backlog-size"))
			Expect(ok).To(BeTrue())
			limits, ok := parseClientOutputBufferLimitClasses(valueOf(params, "client-output-buffer-limit"))
			Expect(ok).To(BeTrue())
			Expect(backlog).To(BeNumerically("<=", limits["replica"].hard), "memory %s", mem)
		}
	})

	It("emits only dual-channel when there is no memory limit to derive from", func() {
		params := managedDefaultParameters(cluster("ghcr.io/halter/valkey-server:8.0.5", "", ""))
		Expect(names(params)).To(Equal([]string{"dual-channel-replication-enabled"}))
	})
})

var _ = Describe("managedConfigEntries", func() {
	It("orders operator defaults before spec parameters so spec wins", func() {
		vc := &cachev1alpha1.ValkeyCluster{
			Spec: cachev1alpha1.ValkeyClusterSpec{
				Image: "ghcr.io/halter/valkey-server:8.0.5",
				ValkeyConfig: &cachev1alpha1.ValkeyConfig{
					Parameters: []cachev1alpha1.ValkeyConfigParameter{
						{Name: "dual-channel-replication-enabled", Value: "no"},
					},
				},
			},
		}
		entries := managedConfigEntries(vc)
		Expect(len(entries)).To(Equal(2))
		Expect(entries[0]).To(Equal(cachev1alpha1.ValkeyConfigParameter{Name: "dual-channel-replication-enabled", Value: "yes"}))
		Expect(entries[1]).To(Equal(cachev1alpha1.ValkeyConfigParameter{Name: "dual-channel-replication-enabled", Value: "no"}))
	})

	It("returns only spec parameters when rawConfig is set", func() {
		vc := &cachev1alpha1.ValkeyCluster{
			Spec: cachev1alpha1.ValkeyClusterSpec{
				Image: "ghcr.io/halter/valkey-server:8.0.5",
				ValkeyConfig: &cachev1alpha1.ValkeyConfig{
					RawConfig: "port 6379",
					Parameters: []cachev1alpha1.ValkeyConfigParameter{
						{Name: "maxmemory", Value: "100mb"},
					},
				},
			},
		}
		Expect(managedConfigEntries(vc)).To(Equal([]cachev1alpha1.ValkeyConfigParameter{
			{Name: "maxmemory", Value: "100mb"},
		}))
	})
})

var _ = Describe("effectiveManagedConfig", func() {
	It("keeps unique directives in order", func() {
		entries := []cachev1alpha1.ValkeyConfigParameter{
			{Name: "maxmemory", Value: "32mb"},
			{Name: "maxmemory-policy", Value: "allkeys-lru"},
		}
		Expect(effectiveManagedConfig(entries)).To(Equal(entries))
	})

	It("collapses a duplicated directive to the later value at the earlier position", func() {
		Expect(effectiveManagedConfig([]cachev1alpha1.ValkeyConfigParameter{
			{Name: "repl-backlog-size", Value: "20578304"},
			{Name: "maxmemory", Value: "32mb"},
			{Name: "repl-backlog-size", Value: "16mb"},
		})).To(Equal([]cachev1alpha1.ValkeyConfigParameter{
			{Name: "repl-backlog-size", Value: "16mb"},
			{Name: "maxmemory", Value: "32mb"},
		}))
	})

	It("merges client-output-buffer-limit per class instead of replacing it", func() {
		Expect(effectiveManagedConfig([]cachev1alpha1.ValkeyConfigParameter{
			{Name: "client-output-buffer-limit", Value: "replica 164626432 82313216 120"},
			{Name: "client-output-buffer-limit", Value: "pubsub 32mb 8mb 60"},
		})).To(Equal([]cachev1alpha1.ValkeyConfigParameter{
			{Name: "client-output-buffer-limit", Value: "replica 164626432 82313216 120 pubsub 32mb 8mb 60"},
		}))
	})

	It("lets a spec class override the defaulted class, slave spelling included", func() {
		Expect(effectiveManagedConfig([]cachev1alpha1.ValkeyConfigParameter{
			{Name: "client-output-buffer-limit", Value: "replica 164626432 82313216 120"},
			{Name: "client-output-buffer-limit", Value: "slave 2gb 1gb 60"},
		})).To(Equal([]cachev1alpha1.ValkeyConfigParameter{
			{Name: "client-output-buffer-limit", Value: "replica 2gb 1gb 60"},
		}))
	})

	It("lets a malformed client-output-buffer-limit override win wholesale", func() {
		Expect(effectiveManagedConfig([]cachev1alpha1.ValkeyConfigParameter{
			{Name: "client-output-buffer-limit", Value: "replica 164626432 82313216 120"},
			{Name: "client-output-buffer-limit", Value: "replica 100"},
		})).To(Equal([]cachev1alpha1.ValkeyConfigParameter{
			{Name: "client-output-buffer-limit", Value: "replica 100"},
		}))
	})
})

var _ = Describe("getValkeyConfigContent with managed defaults", func() {
	It("renders defaults after the base config and before spec parameters", func() {
		vc := &cachev1alpha1.ValkeyCluster{
			Spec: cachev1alpha1.ValkeyClusterSpec{
				Image:    "ghcr.io/halter/valkey-server:8.0.5",
				Password: "hunter2",
				Resources: &corev1.ResourceRequirements{
					Limits: corev1.ResourceList{
						corev1.ResourceMemory: resource.MustParse("16000Mi"),
					},
				},
				ValkeyConfig: &cachev1alpha1.ValkeyConfig{
					Parameters: []cachev1alpha1.ValkeyConfigParameter{
						{Name: "repl-backlog-size", Value: "1073741824"},
					},
				},
			},
		}
		out, err := getValkeyConfigContent(vc)
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(ContainSubstring("cluster-enabled yes"))
		Expect(out).To(ContainSubstring("requirepass hunter2"))
		Expect(out).To(ContainSubstring("\ndual-channel-replication-enabled yes"))
		Expect(out).To(ContainSubstring("\nrepl-backlog-size 536870912"))
		Expect(out).To(ContainSubstring("\nclient-output-buffer-limit replica 4294967296 2147483648 120"))
		// The spec parameter is rendered after the default, so it wins under
		// valkey.conf last-occurrence semantics.
		defaultIdx := strings.Index(out, "repl-backlog-size 536870912")
		specIdx := strings.Index(out, "repl-backlog-size 1073741824")
		Expect(specIdx).To(BeNumerically(">", defaultIdx))
	})

	It("renders no defaults when rawConfig is set", func() {
		vc := &cachev1alpha1.ValkeyCluster{
			Spec: cachev1alpha1.ValkeyClusterSpec{
				Image: "ghcr.io/halter/valkey-server:8.0.5",
				ValkeyConfig: &cachev1alpha1.ValkeyConfig{
					RawConfig: "port 6379",
				},
			},
		}
		out, err := getValkeyConfigContent(vc)
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(Equal("port 6379"))
	})

	It("renders no defaults for images without positive version support", func() {
		vc := &cachev1alpha1.ValkeyCluster{
			Spec: cachev1alpha1.ValkeyClusterSpec{
				Image: "ghcr.io/halter/valkey-server:7.2.5",
			},
		}
		out, err := getValkeyConfigContent(vc)
		Expect(err).NotTo(HaveOccurred())
		Expect(out).NotTo(ContainSubstring("dual-channel-replication-enabled"))
	})
})
