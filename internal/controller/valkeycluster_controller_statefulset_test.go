package controller

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cachev1alpha1 "github.com/halter/valkey-cluster-operator/api/v1alpha1"
)

var _ = Describe("shardAffinity", func() {
	newCluster := func(keys []string, requireHost *bool) *cachev1alpha1.ValkeyCluster {
		return &cachev1alpha1.ValkeyCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "vk", Namespace: "default"},
			Spec: cachev1alpha1.ValkeyClusterSpec{
				Shards:                   2,
				Replicas:                 2,
				AntiAffinityTopologyKeys: keys,
				RequireHostAntiAffinity:  requireHost,
			},
		}
	}
	shardSelector := &metav1.LabelSelector{MatchLabels: map[string]string{
		"statefulset.kubernetes.io/sts-name": "vk-0",
		"cache/name":                         "vk",
	}}
	falseValue := false

	It("requires the hostname term by default and keeps other keys preferred at full weight", func() {
		affinity := shardAffinity("vk-0", newCluster([]string{"kubernetes.io/hostname", "topology.kubernetes.io/zone"}, nil))
		Expect(affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution).To(ConsistOf(corev1.PodAffinityTerm{
			LabelSelector: shardSelector,
			TopologyKey:   "kubernetes.io/hostname",
		}))
		Expect(affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution).To(ConsistOf(corev1.WeightedPodAffinityTerm{
			Weight: 100,
			PodAffinityTerm: corev1.PodAffinityTerm{
				LabelSelector: shardSelector,
				TopologyKey:   "topology.kubernetes.io/zone",
			},
		}))
	})

	It("requires the hostname term even when it is not listed in antiAffinityTopologyKeys", func() {
		affinity := shardAffinity("vk-0", newCluster(nil, nil))
		Expect(affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution).To(HaveLen(1))
		Expect(affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution[0].TopologyKey).To(Equal("kubernetes.io/hostname"))
		Expect(affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution).To(BeEmpty())
	})

	It("falls back to a preferred hostname term when requireHostAntiAffinity is false", func() {
		affinity := shardAffinity("vk-0", newCluster([]string{"kubernetes.io/hostname"}, &falseValue))
		Expect(affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution).To(BeEmpty())
		Expect(affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution).To(HaveLen(1))
		Expect(affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution[0].PodAffinityTerm.TopologyKey).To(Equal("kubernetes.io/hostname"))
	})

	It("sets no affinity when nothing is requested", func() {
		Expect(shardAffinity("vk-0", newCluster(nil, &falseValue))).To(BeNil())
	})

	It("scopes the selector to one shard so replicas of other shards may share a node", func() {
		affinity := shardAffinity("vk-1", newCluster(nil, nil))
		Expect(affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution[0].LabelSelector.MatchLabels).To(HaveKeyWithValue("statefulset.kubernetes.io/sts-name", "vk-1"))
	})
})

var _ = Describe("valkeyReadinessProbe", func() {
	It("runs the readiness script with the configured initial delay", func() {
		probe := valkeyReadinessProbe(45)
		Expect(probe.Exec.Command).To(Equal([]string{"/bin/sh", "/scripts/readiness.sh"}))
		Expect(probe.TCPSocket).To(BeNil())
		Expect(probe.InitialDelaySeconds).To(Equal(int32(45)))
		Expect(probe.PeriodSeconds).To(Equal(int32(10)))
		Expect(probe.TimeoutSeconds).To(Equal(int32(5)))
		Expect(probe.SuccessThreshold).To(Equal(int32(1)))
		Expect(probe.FailureThreshold).To(Equal(int32(3)))
	})
})

var _ = Describe("statefulSet template", func() {
	It("mounts the scripts and uses the readiness script and shard affinity", func() {
		r := &ValkeyClusterReconciler{}
		vc := &cachev1alpha1.ValkeyCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "vk", Namespace: "default"},
			Spec: cachev1alpha1.ValkeyClusterSpec{
				Shards:                   1,
				Replicas:                 2,
				AntiAffinityTopologyKeys: []string{"kubernetes.io/hostname", "topology.kubernetes.io/zone"},
				InitialDelaySeconds:      30,
				Resources:                &corev1.ResourceRequirements{},
			},
		}
		sts := r.statefulSet("vk-0", statefulSetSize(vc), vc)
		Expect(sts.Spec.Template.Spec.Affinity).To(Equal(shardAffinity("vk-0", vc)))
		Expect(sts.Spec.Template.Spec.Containers[0].ReadinessProbe).To(Equal(valkeyReadinessProbe(30)))
		Expect(sts.Spec.Template.Spec.Containers[0].LivenessProbe.TCPSocket).NotTo(BeNil())
	})
})
