package controller

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	cachev1alpha1 "github.com/halter/valkey-cluster-operator/api/v1alpha1"
)

func requestedStorage(requests corev1.ResourceList) string {
	size := requests[corev1.ResourceStorage]
	return size.String()
}

var _ = Describe("desiredStorageSize", func() {
	sizeOf := func(vc *cachev1alpha1.ValkeyCluster) string {
		size := desiredStorageSize(vc)
		return size.String()
	}
	cluster := func(specSize, statusSize string) *cachev1alpha1.ValkeyCluster {
		vc := &cachev1alpha1.ValkeyCluster{}
		if specSize != "" {
			vc.Spec.Storage = &corev1.PersistentVolumeClaimSpec{
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse(specSize),
					},
				},
			}
		}
		if statusSize != "" {
			size := resource.MustParse(statusSize)
			vc.Status.StorageSize = &size
		}
		return vc
	}

	It("defaults to 1Gi when no storage is configured", func() {
		Expect(sizeOf(cluster("", ""))).To(Equal("1Gi"))
	})

	It("uses the spec request when set", func() {
		Expect(sizeOf(cluster("2Gi", ""))).To(Equal("2Gi"))
	})

	It("honours a spec request below the default", func() {
		Expect(sizeOf(cluster("512Mi", ""))).To(Equal("512Mi"))
	})

	It("uses the auto-scaled status size when it exceeds the spec", func() {
		Expect(sizeOf(cluster("2Gi", "3Gi"))).To(Equal("3Gi"))
	})

	It("never shrinks below the spec request", func() {
		Expect(sizeOf(cluster("4Gi", "3Gi"))).To(Equal("4Gi"))
	})

	It("uses the status size with no spec storage", func() {
		Expect(sizeOf(cluster("", "5Gi"))).To(Equal("5Gi"))
	})
})

var _ = Describe("storagePVCSpec", func() {
	It("applies defaults when spec.storage is unset", func() {
		spec := storagePVCSpec(&cachev1alpha1.ValkeyCluster{})
		Expect(spec.AccessModes).To(Equal([]corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}))
		Expect(spec.StorageClassName).To(BeNil())
		Expect(requestedStorage(spec.Resources.Requests)).To(Equal("1Gi"))
	})

	It("keeps user-provided class and access modes, overriding only the size", func() {
		className := "gp3"
		size := resource.MustParse("6Gi")
		vc := &cachev1alpha1.ValkeyCluster{
			Spec: cachev1alpha1.ValkeyClusterSpec{
				Storage: &corev1.PersistentVolumeClaimSpec{
					AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
					StorageClassName: &className,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("2Gi"),
						},
					},
				},
			},
		}
		vc.Status.StorageSize = &size

		spec := storagePVCSpec(vc)
		Expect(spec.AccessModes).To(Equal([]corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}))
		Expect(*spec.StorageClassName).To(Equal("gp3"))
		Expect(requestedStorage(spec.Resources.Requests)).To(Equal("6Gi"))
	})

	It("does not mutate the user-provided storage spec", func() {
		vc := &cachev1alpha1.ValkeyCluster{
			Spec: cachev1alpha1.ValkeyClusterSpec{
				Storage: &corev1.PersistentVolumeClaimSpec{
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("2Gi"),
						},
					},
				},
			},
		}
		size := resource.MustParse("6Gi")
		vc.Status.StorageSize = &size

		_ = storagePVCSpec(vc)
		Expect(requestedStorage(vc.Spec.Storage.Resources.Requests)).To(Equal("2Gi"))
	})
})

var _ = Describe("nextStorageSize", func() {
	next := func(in string) string {
		out := nextStorageSize(resource.MustParse(in))
		return out.String()
	}

	It("grows 1Gi to 2Gi (rounded up to a whole Gi)", func() {
		Expect(next("1Gi")).To(Equal("2Gi"))
	})

	It("grows 2Gi to 3Gi", func() {
		Expect(next("2Gi")).To(Equal("3Gi"))
	})

	It("grows 3Gi to 5Gi", func() {
		Expect(next("3Gi")).To(Equal("5Gi"))
	})

	It("grows 10Gi to 15Gi", func() {
		Expect(next("10Gi")).To(Equal("15Gi"))
	})

	It("rounds sub-Gi sizes up to a whole Gi", func() {
		Expect(next("512Mi")).To(Equal("1Gi"))
	})
})

var _ = Describe("parseDfUsedPercent", func() {
	It("computes usage from used and available blocks", func() {
		out := `Filesystem     1024-blocks    Used Available Capacity Mounted on
/dev/nvme1n1       1038336  248560    789776      24% /data
`
		Expect(parseDfUsedPercent(out)).To(Equal(23))
	})

	It("parses busybox-style df output", func() {
		out := `Filesystem           1024-blocks    Used Available Capacity Mounted on
/dev/sda1              10218772 5109386   5109386  50% /data
`
		Expect(parseDfUsedPercent(out)).To(Equal(50))
	})

	It("errors on missing data line", func() {
		_, err := parseDfUsedPercent("Filesystem 1024-blocks Used Available Capacity Mounted on\n")
		Expect(err).To(HaveOccurred())
	})

	It("errors on malformed output", func() {
		_, err := parseDfUsedPercent("header\nnot a df line\n")
		Expect(err).To(HaveOccurred())
	})

	It("errors when no capacity is reported", func() {
		out := `Filesystem     1024-blocks    Used Available Capacity Mounted on
/dev/nvme1n1             0       0         0       0% /data
`
		_, err := parseDfUsedPercent(out)
		Expect(err).To(HaveOccurred())
	})
})
