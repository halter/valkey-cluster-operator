package controller

import (
	"errors"
	"strings"

	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cachev1alpha1 "github.com/halter/valkey-cluster-operator/api/v1alpha1"
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

var _ = Describe("isJobFinished", func() {
	It("reports an in-flight Job as not finished", func() {
		Expect(isJobFinished(&batchv1.Job{})).To(BeFalse())
		Expect(isJobFinished(&batchv1.Job{Status: batchv1.JobStatus{Active: 1}})).To(BeFalse())
	})

	It("reports succeeded and failed Jobs as finished", func() {
		Expect(isJobFinished(&batchv1.Job{Status: batchv1.JobStatus{Succeeded: 1}})).To(BeTrue())
		Expect(isJobFinished(&batchv1.Job{Status: batchv1.JobStatus{Failed: 1}})).To(BeTrue())
	})
})

var _ = Describe("valkey-cli Job tracking across reconciles", func() {
	// Note: envtest runs only the API server, so created Jobs never get pods and
	// never progress on their own; tests drive Job status transitions manually.
	// Foreground deletion also never completes (no garbage collector), so
	// deletion is asserted via DeletionTimestamp rather than absence.

	newReconciler := func() *ValkeyClusterReconciler {
		return &ValkeyClusterReconciler{
			Client: k8sClient,
			Scheme: k8sClient.Scheme(),
		}
	}

	newCluster := func(name string) *cachev1alpha1.ValkeyCluster {
		return &cachev1alpha1.ValkeyCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: "default",
			},
			Spec: cachev1alpha1.ValkeyClusterSpec{
				Shards:   1,
				Replicas: 0,
				Image:    "valkey/valkey:8.0.2",
			},
		}
	}

	createJob := func(r *ValkeyClusterReconciler, valkeyCluster *cachev1alpha1.ValkeyCluster, timestamp int64) *batchv1.Job {
		job := r.buildValkeyCliJob(
			valkeyCliJobName(valkeyCluster.Name, timestamp),
			valkeyCluster,
			[]string{"--cluster", "check", "127.0.0.1:6379"})
		Expect(k8sClient.Create(ctx, job)).To(Succeed())
		return job
	}

	markSucceeded := func(job *batchv1.Job) {
		job.Status.Succeeded = 1
		Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())
	}

	markFailed := func(job *batchv1.Job) {
		job.Status.Failed = 1
		Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())
	}

	listJobs := func(valkeyCluster *cachev1alpha1.ValkeyCluster) []batchv1.Job {
		jobList := &batchv1.JobList{}
		Expect(k8sClient.List(ctx, jobList,
			client.InNamespace(valkeyCluster.Namespace),
			client.MatchingLabels(valkeyCliJobLabels(valkeyCluster.Name)))).To(Succeed())
		return jobList.Items
	}

	Describe("executeValkeyCliJob", func() {
		It("does not create a competing Job while one is still running", func() {
			r := newReconciler()
			valkeyCluster := newCluster("track-exec")

			createJob(r, valkeyCluster, 1789000001)

			_, _, err := r.executeValkeyCliJob(ctx, valkeyCluster, []string{"--cluster", "check", "127.0.0.1:6379"})
			Expect(errors.Is(err, errValkeyCliJobStillRunning)).To(BeTrue())
			Expect(listJobs(valkeyCluster)).To(HaveLen(1))
		})
	})

	Describe("settleValkeyCliJobs", func() {
		It("reaps finished leftovers and reports a still-running one", func() {
			r := newReconciler()
			valkeyCluster := newCluster("track-settle")
			otherCluster := newCluster("track-settle-other")

			running := createJob(r, valkeyCluster, 1789000001)
			finished := createJob(r, valkeyCluster, 1789000002)
			markFailed(finished)
			// Another cluster's running Job must not be picked up.
			otherRunning := createJob(r, otherCluster, 1789000003)

			found, err := r.settleValkeyCliJobs(ctx, valkeyCluster, logr.Discard())
			Expect(err).NotTo(HaveOccurred())
			Expect(found).NotTo(BeNil())
			Expect(found.Name).To(Equal(running.Name))

			// The still-running Job must NOT have been deleted.
			runningAfter := &batchv1.Job{}
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(running), runningAfter)).To(Succeed())
			Expect(runningAfter.DeletionTimestamp).To(BeNil())

			// The finished leftover has been reaped.
			finishedAfter := &batchv1.Job{}
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(finished), finishedAfter)).To(Succeed())
			Expect(finishedAfter.DeletionTimestamp).NotTo(BeNil())

			// The other cluster's Job is untouched.
			otherAfter := &batchv1.Job{}
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(otherRunning), otherAfter)).To(Succeed())
			Expect(otherAfter.DeletionTimestamp).To(BeNil())

			// Once the running Job finishes, nothing is reported as running.
			markSucceeded(running)
			found, err = r.settleValkeyCliJobs(ctx, valkeyCluster, logr.Discard())
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeNil())
		})
	})
})
