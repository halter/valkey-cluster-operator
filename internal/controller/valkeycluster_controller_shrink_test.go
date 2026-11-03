package controller

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	internalValkey "github.com/halter/valkey-cluster-operator/internal/controller/valkey"
)

var _ = Describe("planShardShrink", func() {
	primary := func(pod, id string) *internalValkey.ClusterNode {
		return &internalValkey.ClusterNode{Pod: pod, ID: id, IP: pod + "-ip", Flags: []string{"master"}}
	}
	replica := func(pod, id, masterID string) *internalValkey.ClusterNode {
		return &internalValkey.ClusterNode{Pod: pod, ID: id, IP: pod + "-ip", MasterNodeID: masterID, Flags: []string{"slave"}}
	}

	It("shrinks when every removed pod is a replica", func() {
		nodes := []*internalValkey.ClusterNode{primary("vk-0-0", "a"), replica("vk-0-1", "b", "a"), replica("vk-0-2", "c", "a")}
		Expect(planShardShrink("vk-0", 1, 3, nodes)).To(Equal(shardShrinkStep{}))
	})

	It("fails a removed primary over to a replica that stays", func() {
		nodes := []*internalValkey.ClusterNode{replica("vk-0-0", "a", "c"), replica("vk-0-1", "b", "c"), primary("vk-0-2", "c")}
		step := planShardShrink("vk-0", 2, 3, nodes)
		Expect(step.failoverTo).NotTo(BeNil())
		Expect(step.failoverTo.Pod).To(BeElementOf("vk-0-0", "vk-0-1"))
		Expect(step.primaryPod).To(Equal("vk-0-2"))
	})

	It("never fails over to a replica that is itself removed", func() {
		nodes := []*internalValkey.ClusterNode{replica("vk-0-0", "a", "b"), primary("vk-0-1", "b"), replica("vk-0-2", "c", "b")}
		step := planShardShrink("vk-0", 1, 3, nodes)
		Expect(step.failoverTo.Pod).To(Equal("vk-0-0"))
	})

	It("waits when a removed primary has no replica that stays", func() {
		nodes := []*internalValkey.ClusterNode{primary("vk-0-0", "a"), primary("vk-0-1", "b")}
		step := planShardShrink("vk-0", 1, 2, nodes)
		Expect(step.failoverTo).To(BeNil())
		Expect(step.wait).To(ContainSubstring("primary vk-0-1 has no replica among the pods that stay"))
	})

	It("waits when a removed pod's role cannot be read", func() {
		nodes := []*internalValkey.ClusterNode{primary("vk-0-0", "a"), replica("vk-0-1", "b", "a")}
		step := planShardShrink("vk-0", 1, 3, nodes)
		Expect(step.wait).To(ContainSubstring("pod vk-0-2 is not a Ready cluster node"))
	})

	It("reads only its own shard's pods", func() {
		nodes := []*internalValkey.ClusterNode{primary("vk-0-0", "a"), replica("vk-0-1", "b", "a"), primary("vk-1-1", "x")}
		Expect(planShardShrink("vk-0", 1, 2, nodes)).To(Equal(shardShrinkStep{}))
	})
})
