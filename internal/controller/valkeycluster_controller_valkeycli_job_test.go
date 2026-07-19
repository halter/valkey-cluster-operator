package controller

import (
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/util/validation"
)

var _ = Describe("valkeyCliJobName", func() {
	const timestamp = int64(1789000000)

	It("uses the cluster name unchanged when the result fits", func() {
		Expect(valkeyCliJobName("my-cluster", timestamp)).
			To(Equal("my-cluster-valkey-cli-1789000000"))
	})

	It("stays within 63 characters for long cluster names", func() {
		longName := strings.Repeat("a", 45) + "-cluster"
		name := valkeyCliJobName(longName, timestamp)
		Expect(len(name)).To(BeNumerically("<=", maxJobNameLength))
		Expect(validation.IsDNS1123Label(name)).To(BeEmpty())
		Expect(name).To(HavePrefix("aaaa"))
		Expect(name).To(HaveSuffix("-valkey-cli-1789000000"))
	})

	It("produces distinct names for long cluster names sharing a prefix", func() {
		sharedPrefix := strings.Repeat("a", 60)
		nameOne := valkeyCliJobName(sharedPrefix+"-one", timestamp)
		nameTwo := valkeyCliJobName(sharedPrefix+"-two", timestamp)
		Expect(nameOne).NotTo(Equal(nameTwo))
	})

	It("remains a valid DNS-1123 label when truncation lands on a dash", func() {
		// Choose a name whose truncation point falls right after a dash so the
		// untrimmed prefix would end in one.
		longName := strings.Repeat("a", 31) + "-" + strings.Repeat("b", 20)
		name := valkeyCliJobName(longName, timestamp)
		Expect(len(name)).To(BeNumerically("<=", maxJobNameLength))
		Expect(validation.IsDNS1123Label(name)).To(BeEmpty())
		Expect(name).NotTo(ContainSubstring("--"))
	})
})
