package controller

import (
	"encoding/json"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cachev1alpha1 "github.com/halter/valkey-cluster-operator/api/v1alpha1"
)

const testNamespace = "default"

func resources(pairs ...string) corev1.ResourceList {
	list := corev1.ResourceList{}
	for i := 0; i+1 < len(pairs); i += 2 {
		list[corev1.ResourceName(pairs[i])] = resource.MustParse(pairs[i+1])
	}
	return list
}

func quantityOf(list corev1.ResourceList, name corev1.ResourceName) string {
	q, ok := list[name]
	if !ok {
		return "<unset>"
	}
	return q.String()
}

func autoscaledCluster(requests, limits, maxResources, status corev1.ResourceList) *cachev1alpha1.ValkeyCluster {
	return &cachev1alpha1.ValkeyCluster{
		Spec: cachev1alpha1.ValkeyClusterSpec{
			Resources:   &corev1.ResourceRequirements{Requests: requests, Limits: limits},
			Autoscaling: &cachev1alpha1.ValkeyClusterAutoscaling{MaxResources: maxResources},
		},
		Status: cachev1alpha1.ValkeyClusterStatus{AutoscaledResources: status},
	}
}

var _ = Describe("effectiveResources", func() {
	requests := resources("cpu", "100m", "memory", "256Mi")
	limits := resources("cpu", "200m", "memory", "512Mi")
	maxResources := resources("cpu", "1", "memory", "1Gi")

	It("scales requests and limits by the same ratio", func() {
		got := effectiveResources(autoscaledCluster(requests, limits, maxResources, resources("cpu", "400m", "memory", "896Mi")))
		Expect(quantityOf(got.Limits, corev1.ResourceMemory)).To(Equal("896Mi"))
		Expect(quantityOf(got.Requests, corev1.ResourceMemory)).To(Equal("448Mi"))
		Expect(quantityOf(got.Limits, corev1.ResourceCPU)).To(Equal("400m"))
		Expect(quantityOf(got.Requests, corev1.ResourceCPU)).To(Equal("200m"))
	})

	It("rounds scaled memory up to a whole Mi and cpu up to a whole millicore", func() {
		got := effectiveResources(autoscaledCluster(
			resources("cpu", "33m", "memory", "100Mi"), resources("cpu", "100m", "memory", "300Mi"),
			maxResources, resources("cpu", "150m", "memory", "448Mi")))
		Expect(quantityOf(got.Requests, corev1.ResourceMemory)).To(Equal("150Mi"))
		Expect(quantityOf(got.Requests, corev1.ResourceCPU)).To(Equal("50m"))
	})

	It("clamps the size to spec.resources and maxResources", func() {
		below := effectiveResources(autoscaledCluster(requests, limits, maxResources, resources("memory", "128Mi")))
		Expect(quantityOf(below.Limits, corev1.ResourceMemory)).To(Equal("512Mi"))
		Expect(quantityOf(below.Requests, corev1.ResourceMemory)).To(Equal("256Mi"))

		above := effectiveResources(autoscaledCluster(requests, limits, maxResources, resources("memory", "4Gi")))
		Expect(quantityOf(above.Limits, corev1.ResourceMemory)).To(Equal("1Gi"))
		Expect(quantityOf(above.Requests, corev1.ResourceMemory)).To(Equal("512Mi"))
	})

	It("leaves a resource that is not auto-scaled as in spec", func() {
		got := effectiveResources(autoscaledCluster(requests, limits, resources("memory", "1Gi"), resources("cpu", "400m")))
		Expect(quantityOf(got.Limits, corev1.ResourceCPU)).To(Equal("200m"))
		Expect(quantityOf(got.Requests, corev1.ResourceCPU)).To(Equal("100m"))
	})

	It("returns empty requirements when spec.resources is unset", func() {
		got := effectiveResources(&cachev1alpha1.ValkeyCluster{})
		Expect(got.Requests).To(BeEmpty())
		Expect(got.Limits).To(BeEmpty())
	})

	It("scales a resource that only has a request", func() {
		got := effectiveResources(autoscaledCluster(resources("memory", "256Mi"), nil, maxResources, resources("memory", "512Mi")))
		Expect(quantityOf(got.Requests, corev1.ResourceMemory)).To(Equal("512Mi"))
		Expect(got.Limits).To(BeEmpty())
	})
})

var _ = Describe("planResourceScaling", func() {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	requests := resources("cpu", "100m", "memory", "256Mi")
	limits := resources("cpu", "200m", "memory", "512Mi")
	maxResources := resources("cpu", "1", "memory", "1Gi")

	scaledAt := func(vc *cachev1alpha1.ValkeyCluster, ago time.Duration) *cachev1alpha1.ValkeyCluster {
		vc.Status.LastResourceScaleTime = &metav1.Time{Time: now.Add(-ago)}
		return vc
	}
	planFor := func(vc *cachev1alpha1.ValkeyCluster, name corev1.ResourceName, usage corev1.ResourceList, oomKilled bool) (corev1.ResourceList, resourceScaling) {
		sizes, plan := planResourceScaling(vc, usage, oomKilled, now)
		for _, s := range plan {
			if s.name == name {
				return sizes, s
			}
		}
		Fail("no decision for " + string(name))
		return nil, resourceScaling{}
	}

	It("initialises status to the size in spec.resources", func() {
		sizes, _ := planResourceScaling(autoscaledCluster(requests, limits, maxResources, nil), nil, false, now)
		Expect(quantityOf(sizes, corev1.ResourceMemory)).To(Equal("512Mi"))
		Expect(quantityOf(sizes, corev1.ResourceCPU)).To(Equal("200m"))
	})

	It("scales up to twice the usage, rounded up to 64Mi", func() {
		sizes, s := planFor(autoscaledCluster(requests, limits, maxResources, nil), corev1.ResourceMemory, resources("memory", "420Mi"), false)
		Expect(quantityOf(sizes, corev1.ResourceMemory)).To(Equal("896Mi"))
		Expect(s.scaled()).To(BeTrue())
		Expect(s.eventMessage("")).To(Equal("Memory usage reached 82% of 512Mi, scaling memory to 896Mi"))
	})

	It("keeps the size while usage is between 25% and 75%", func() {
		sizes, s := planFor(autoscaledCluster(requests, limits, maxResources, nil), corev1.ResourceMemory, resources("memory", "384Mi"), false)
		Expect(quantityOf(sizes, corev1.ResourceMemory)).To(Equal("512Mi"))
		Expect(s.scaled()).To(BeFalse())
	})

	It("scales down only 30 minutes after the last scale", func() {
		floor := resources("memory", "256Mi")
		usage := resources("memory", "150Mi")
		recent := scaledAt(autoscaledCluster(nil, floor, maxResources, resources("memory", "896Mi")), 10*time.Minute)
		sizes, _ := planFor(recent, corev1.ResourceMemory, usage, false)
		Expect(quantityOf(sizes, corev1.ResourceMemory)).To(Equal("896Mi"))

		old := scaledAt(autoscaledCluster(nil, floor, maxResources, resources("memory", "896Mi")), 30*time.Minute)
		sizes, _ = planFor(old, corev1.ResourceMemory, usage, false)
		Expect(quantityOf(sizes, corev1.ResourceMemory)).To(Equal("320Mi"))
	})

	It("never scales below the size in spec.resources", func() {
		vc := scaledAt(autoscaledCluster(requests, limits, maxResources, resources("cpu", "800m")), time.Hour)
		sizes, s := planFor(vc, corev1.ResourceCPU, resources("cpu", "32m"), false)
		Expect(quantityOf(sizes, corev1.ResourceCPU)).To(Equal("200m"))
		Expect(s.eventMessage("")).To(Equal("CPU usage fell to 4% of 800m, scaling cpu to 200m"))
	})

	It("stops at maxResources and reports the resource as limited once there", func() {
		vc := autoscaledCluster(requests, limits, maxResources, nil)
		sizes, s := planFor(vc, corev1.ResourceMemory, resources("memory", "500Mi"), false)
		Expect(quantityOf(sizes, corev1.ResourceMemory)).To(Equal("1Gi"))
		Expect(s.limited).To(BeFalse())

		vc = autoscaledCluster(requests, limits, maxResources, resources("memory", "1Gi"))
		sizes, plan := planResourceScaling(vc, resources("memory", "900Mi"), false, now)
		Expect(quantityOf(sizes, corev1.ResourceMemory)).To(Equal("1Gi"))
		condition := resourcesLimitedCondition(vc, plan)
		Expect(condition).NotTo(BeNil())
		Expect(condition.Status).To(Equal(metav1.ConditionTrue))
		Expect(condition.Reason).To(Equal("MaxResourcesReached"))
		Expect(condition.Message).To(ContainSubstring("memory (1Gi)"))
	})

	It("clears the limited condition once usage fits", func() {
		vc := autoscaledCluster(requests, limits, maxResources, resources("memory", "1Gi"))
		vc.Status.Conditions = []metav1.Condition{{Type: typeResourcesLimitedValkeyCluster, Status: metav1.ConditionTrue, Reason: "MaxResourcesReached"}}
		_, plan := planResourceScaling(vc, resources("memory", "600Mi"), false, now)
		condition := resourcesLimitedCondition(vc, plan)
		Expect(condition).NotTo(BeNil())
		Expect(condition.Status).To(Equal(metav1.ConditionFalse))
	})

	It("grows memory by half after an OOM kill", func() {
		vc := scaledAt(autoscaledCluster(requests, limits, maxResources, nil), time.Minute)
		sizes, s := planFor(vc, corev1.ResourceMemory, nil, true)
		Expect(quantityOf(sizes, corev1.ResourceMemory)).To(Equal("768Mi"))
		Expect(s.eventMessage("cluster-0-1")).To(Equal("valkey-cluster-node in pod cluster-0-1 was OOM-killed, scaling memory from 512Mi to 768Mi"))
	})

	It("detects only OOM kills after the last scale", func() {
		pod := &corev1.Pod{Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
			Name: valkeyContainerName,
			LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
				Reason: "OOMKilled", FinishedAt: metav1.NewTime(now),
			}},
		}}}}
		Expect(oomKilledSince(pod, nil)).To(BeTrue())
		Expect(oomKilledSince(pod, &metav1.Time{Time: now.Add(-time.Minute)})).To(BeTrue())
		Expect(oomKilledSince(pod, &metav1.Time{Time: now.Add(time.Minute)})).To(BeFalse())
	})
})

var _ = Describe("nextCPUSamples", func() {
	t0 := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	const uid = types.UID("pod-a")
	const runID = "run-1"
	previous := map[types.UID]cpuSample{uid: {runID: runID, seconds: 10, at: t0}}

	It("measures the rate between samples of the same process", func() {
		next, rate := nextCPUSamples(previous, map[types.UID]cpuSample{uid: {runID: runID, seconds: 70, at: t0.Add(2 * time.Minute)}})
		Expect(rate).NotTo(BeNil())
		Expect(rate.String()).To(Equal("500m"))
		Expect(next[uid].seconds).To(Equal(70.0))
	})

	It("resets the baseline when the process restarted", func() {
		reading := cpuSample{runID: "run-2", seconds: 1, at: t0.Add(2 * time.Minute)}
		next, rate := nextCPUSamples(previous, map[types.UID]cpuSample{uid: reading})
		Expect(rate).To(BeNil())
		Expect(next[uid]).To(Equal(reading))

		lower := cpuSample{runID: runID, seconds: 5, at: t0.Add(2 * time.Minute)}
		next, rate = nextCPUSamples(previous, map[types.UID]cpuSample{uid: lower})
		Expect(rate).To(BeNil())
		Expect(next[uid]).To(Equal(lower))
	})

	It("keeps the old sample until the window reaches a minute", func() {
		next, rate := nextCPUSamples(previous, map[types.UID]cpuSample{uid: {runID: runID, seconds: 20, at: t0.Add(30 * time.Second)}})
		Expect(rate).To(BeNil())
		Expect(next[uid]).To(Equal(previous[uid]))

		_, rate = nextCPUSamples(next, map[types.UID]cpuSample{uid: {runID: runID, seconds: 55, at: t0.Add(90 * time.Second)}})
		Expect(rate).NotTo(BeNil())
		Expect(rate.String()).To(Equal("500m"))
	})

	It("drops pods that were not measured", func() {
		next, _ := nextCPUSamples(previous, map[types.UID]cpuSample{})
		Expect(next).To(BeEmpty())
	})
})

func controllerRevision(name string, template corev1.PodTemplateSpec) *appsv1.ControllerRevision {
	raw, err := json.Marshal(template)
	Expect(err).NotTo(HaveOccurred())
	var encoded map[string]any
	Expect(json.Unmarshal(raw, &encoded)).To(Succeed())
	encoded["$patch"] = "replace"
	data, err := json.Marshal(map[string]any{"spec": map[string]any{"template": encoded}})
	Expect(err).NotTo(HaveOccurred())
	return &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
		Data:       runtime.RawExtension{Raw: data},
		Revision:   1,
	}
}

func revisionTestCluster(name string) *cachev1alpha1.ValkeyCluster {
	return &cachev1alpha1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
		Spec: cachev1alpha1.ValkeyClusterSpec{
			Shards: 1,
			Image:  "valkey-server:8.0.5",
			Resources: &corev1.ResourceRequirements{
				Requests: resources("cpu", "100m", "memory", "256Mi"),
				Limits:   resources("cpu", "200m", "memory", "512Mi"),
			},
			Autoscaling: &cachev1alpha1.ValkeyClusterAutoscaling{MaxResources: resources("memory", "1Gi")},
		},
	}
}

var _ = Describe("resourceOnlyRevisionChange", func() {
	r := &ValkeyClusterReconciler{}
	template := r.statefulSet("rev-0", 1, revisionTestCluster("rev")).Spec.Template

	It("accepts revisions that differ only in the valkey container's resources", func() {
		resized := *template.DeepCopy()
		resized.Spec.Containers[0].Resources.Limits = resources("cpu", "400m", "memory", "896Mi")
		Expect(resourceOnlyRevisionChange(controllerRevision("a", template), controllerRevision("b", resized))).To(BeTrue())
	})

	It("rejects revisions that differ in image or env", func() {
		image := *template.DeepCopy()
		image.Spec.Containers[0].Image = "valkey-server:9.0.1"
		Expect(resourceOnlyRevisionChange(controllerRevision("a", template), controllerRevision("b", image))).To(BeFalse())

		env := *template.DeepCopy()
		env.Spec.Containers[0].Env = append(env.Spec.Containers[0].Env, corev1.EnvVar{Name: "EXTRA", Value: "1"})
		Expect(resourceOnlyRevisionChange(controllerRevision("a", template), controllerRevision("b", env))).To(BeFalse())
	})

	It("rejects a missing revision", func() {
		Expect(resourceOnlyRevisionChange(nil, controllerRevision("b", template))).To(BeFalse())
	})
})

var _ = Describe("reconcilePodRevisions", func() {
	setup := func(name string, change func(*corev1.PodTemplateSpec), podResources corev1.ResourceRequirements) (*ValkeyClusterReconciler, *cachev1alpha1.ValkeyCluster) {
		vc := revisionTestCluster(name)
		Expect(k8sClient.Create(ctx, vc)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, vc)).To(Succeed()) })

		r := &ValkeyClusterReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: record.NewFakeRecorder(100)}
		sts := r.statefulSet(name+"-0", 1, vc)
		Expect(k8sClient.Create(ctx, sts)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, sts)).To(Succeed()) })

		oldTemplate := *sts.Spec.Template.DeepCopy()
		change(&oldTemplate)
		for _, rev := range []*appsv1.ControllerRevision{
			controllerRevision(name+"-old", oldTemplate),
			controllerRevision(name+"-new", sts.Spec.Template),
		} {
			Expect(k8sClient.Create(ctx, rev)).To(Succeed())
			DeferCleanup(func() { Expect(k8sClient.Delete(ctx, rev)).To(Succeed()) })
		}
		sts.Status = appsv1.StatefulSetStatus{ObservedGeneration: sts.Generation, Replicas: 1, CurrentRevision: name + "-old", UpdateRevision: name + "-new"}
		Expect(k8sClient.Status().Update(ctx, sts)).To(Succeed())

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name + "-0-0", Namespace: testNamespace, Labels: map[string]string{}},
			Spec:       *oldTemplate.Spec.DeepCopy(),
		}
		for k, v := range oldTemplate.Labels {
			pod.Labels[k] = v
		}
		pod.Labels[appsv1.ControllerRevisionHashLabelKey] = name + "-old"
		pod.Spec.Containers[0].Resources = podResources
		pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{Name: "valkey-data", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}})
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, pod, client.GracePeriodSeconds(0))).To(Succeed()) })
		return r, vc
	}
	podRevision := func(name string) string {
		pod := &corev1.Pod{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name + "-0-0", Namespace: testNamespace}, pod)).To(Succeed())
		return pod.Labels[appsv1.ControllerRevisionHashLabelKey]
	}
	smaller := func(t *corev1.PodTemplateSpec) {
		t.Spec.Containers[0].Resources.Limits = resources("cpu", "100m", "memory", "256Mi")
	}

	It("restarts the pod when the API server has no resize subresource", func() {
		r, vc := setup("revisions-resize", smaller, corev1.ResourceRequirements{
			Requests: resources("cpu", "100m", "memory", "256Mi"),
			Limits:   resources("cpu", "100m", "memory", "256Mi"),
		})
		restarts, changed, err := r.reconcilePodRevisions(ctx, vc)
		Expect(err).NotTo(HaveOccurred())
		Expect(changed).To(BeFalse())
		Expect(restarts).To(HaveLen(1))
		Expect(restarts[0].Name).To(Equal("revisions-resize-0-0"))
		Expect(podRevision("revisions-resize")).To(Equal("revisions-resize-old"))
	})

	It("relabels without a restart when the pod already has the new resources", func() {
		r, vc := setup("revisions-relabel", smaller, *revisionTestCluster("x").Spec.Resources.DeepCopy())
		restarts, changed, err := r.reconcilePodRevisions(ctx, vc)
		Expect(err).NotTo(HaveOccurred())
		Expect(changed).To(BeTrue())
		Expect(restarts).To(BeEmpty())
		Expect(podRevision("revisions-relabel")).To(Equal("revisions-relabel-new"))
	})

	It("restarts the pod when the revisions differ in more than resources", func() {
		image := func(t *corev1.PodTemplateSpec) { t.Spec.Containers[0].Image = "valkey-server:8.0.2" }
		r, vc := setup("revisions-image", image, *revisionTestCluster("x").Spec.Resources.DeepCopy())
		restarts, changed, err := r.reconcilePodRevisions(ctx, vc)
		Expect(err).NotTo(HaveOccurred())
		Expect(changed).To(BeFalse())
		Expect(restarts).To(HaveLen(1))
		Expect(podRevision("revisions-image")).To(Equal("revisions-image-old"))
	})
})

var _ = Describe("autoscaling validation", func() {
	It("rejects auto-scaling a resource other than cpu and memory", func() {
		vc := revisionTestCluster("invalid-resource")
		vc.Spec.Autoscaling.MaxResources = resources("ephemeral-storage", "10Gi")
		err := k8sClient.Create(ctx, vc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("only cpu and memory can be auto-scaled"))
	})

	It("rejects auto-scaling a resource without a request or limit", func() {
		vc := revisionTestCluster("invalid-floor")
		vc.Spec.Resources = &corev1.ResourceRequirements{Limits: resources("cpu", "200m")}
		err := k8sClient.Create(ctx, vc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("each auto-scaled resource needs a request or limit in spec.resources"))
	})
})
