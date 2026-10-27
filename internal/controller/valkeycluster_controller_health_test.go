package controller

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cachev1alpha1 "github.com/halter/valkey-cluster-operator/api/v1alpha1"
)

var _ = Describe("rollout health gate", func() {
	cluster := func(shards, replicas int32) *cachev1alpha1.ValkeyCluster {
		return &cachev1alpha1.ValkeyCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "vk", Namespace: "default"},
			Spec:       cachev1alpha1.ValkeyClusterSpec{Shards: shards, Replicas: replicas},
		}
	}
	sts := func(name string, want, created, ready int32) appsv1.StatefulSet {
		return appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: name, Generation: 3},
			Spec:       appsv1.StatefulSetSpec{Replicas: &want},
			Status:     appsv1.StatefulSetStatus{ObservedGeneration: 3, Replicas: created, ReadyReplicas: ready},
		}
	}

	It("expects shards times (replicas + 1) pods", func() {
		Expect(expectedPodCount(cluster(2, 2))).To(Equal(6))
		Expect(expectedPodCount(cluster(1, 1))).To(Equal(2))
		Expect(expectedPodCount(cluster(3, 0))).To(Equal(3))
	})

	It("is settled when every StatefulSet has created and readied all its pods", func() {
		_, ok := statefulSetsSettled([]appsv1.StatefulSet{sts("vk-0", 3, 3, 3), sts("vk-1", 3, 3, 3)}, cluster(2, 2))
		Expect(ok).To(BeTrue())
	})

	It("is not settled while a deleted pod has not been recreated yet", func() {
		reason, ok := statefulSetsSettled([]appsv1.StatefulSet{sts("vk-0", 2, 1, 1)}, cluster(1, 1))
		Expect(ok).To(BeFalse())
		Expect(reason).To(ContainSubstring("vk-0 has 1 pods, 1 Ready, wants 2"))
	})

	It("is not settled while a recreated pod is not Ready", func() {
		_, ok := statefulSetsSettled([]appsv1.StatefulSet{sts("vk-0", 2, 2, 1)}, cluster(1, 1))
		Expect(ok).To(BeFalse())
	})

	It("is not settled while a StatefulSet's status lags its generation", func() {
		stale := sts("vk-0", 2, 2, 2)
		stale.Status.ObservedGeneration = 2
		reason, ok := statefulSetsSettled([]appsv1.StatefulSet{stale}, cluster(1, 1))
		Expect(ok).To(BeFalse())
		Expect(reason).To(ContainSubstring("not yet observed"))
	})

	It("is not settled when the number of StatefulSets differs from the shard count", func() {
		_, ok := statefulSetsSettled([]appsv1.StatefulSet{sts("vk-0", 2, 2, 2)}, cluster(2, 1))
		Expect(ok).To(BeFalse())
	})
})
