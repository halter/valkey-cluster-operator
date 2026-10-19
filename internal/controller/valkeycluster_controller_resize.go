package controller

import (
	"context"
	"encoding/json"
	"sort"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cachev1alpha1 "github.com/halter/valkey-cluster-operator/api/v1alpha1"
)

const (
	statefulSetNameLabel = "statefulset.kubernetes.io/sts-name"

	// podResizePendingCondition is set by the kubelet (Kubernetes 1.33 and
	// later) when the node cannot fit a resize yet or at all.
	podResizePendingCondition corev1.PodConditionType = "PodResizePending"
)

// podRevisionOutcome is what reconcilePodRevision did with a pod.
type podRevisionOutcome int

const (
	podUpToDate podRevisionOutcome = iota
	podResizedInPlace
	podNeedsRestart
)

// reconcilePodRevisions brings pods to their StatefulSet's update revision. A
// pod whose revision differs from it only in the valkey container's
// resources is resized in place through the pod's resize subresource and
// relabelled with the update revision. Every other outdated pod, a resize the
// API server refuses and a resize the node cannot fit need a restart. The
// restarts returned belong to the first StatefulSet in name order that needs
// any, so the rolling update finishes one shard before it starts the next.
func (r *ValkeyClusterReconciler) reconcilePodRevisions(ctx context.Context, valkeyCluster *cachev1alpha1.ValkeyCluster) ([]corev1.Pod, bool, error) {
	stsList := &appsv1.StatefulSetList{}
	if err := r.List(ctx, stsList,
		client.InNamespace(valkeyCluster.Namespace),
		client.MatchingLabels(labelsForValkeyCluster(valkeyCluster.Name))); err != nil {
		return nil, false, err
	}
	sort.Slice(stsList.Items, func(i, j int) bool {
		return stsList.Items[i].Name < stsList.Items[j].Name
	})

	var podsNeedingRestart []corev1.Pod
	changed := false
	for i := range stsList.Items {
		sts := &stsList.Items[i]
		if sts.Status.ObservedGeneration < sts.Generation || sts.Status.UpdateRevision == "" {
			continue
		}
		podList := &corev1.PodList{}
		if err := r.List(ctx, podList,
			client.InNamespace(sts.Namespace),
			client.MatchingLabels(sts.Spec.Selector.MatchLabels)); err != nil {
			return nil, false, err
		}
		var restarts []corev1.Pod
		for j := range podList.Items {
			pod := &podList.Items[j]
			if pod.DeletionTimestamp != nil {
				continue
			}
			outcome, err := r.reconcilePodRevision(ctx, sts, pod)
			if err != nil {
				return nil, false, err
			}
			switch outcome {
			case podResizedInPlace:
				changed = true
			case podNeedsRestart:
				restarts = append(restarts, *pod)
			}
		}
		if podsNeedingRestart == nil {
			podsNeedingRestart = restarts
		}
	}
	return podsNeedingRestart, changed, nil
}

// reconcilePodRevision resizes one pod in place when that brings it to the
// StatefulSet's update revision, or reports that it needs a restart.
func (r *ValkeyClusterReconciler) reconcilePodRevision(ctx context.Context, sts *appsv1.StatefulSet, pod *corev1.Pod) (podRevisionOutcome, error) {
	logger := log.FromContext(ctx)

	if podResizePending(pod) {
		logger.Info("Node cannot fit the pod's resize, restarting it", "pod", pod.Name)
		return podNeedsRestart, nil
	}
	podRevision := pod.Labels[appsv1.ControllerRevisionHashLabelKey]
	if podRevision == sts.Status.UpdateRevision {
		return podUpToDate, nil
	}

	from, err := r.getControllerRevision(ctx, sts.Namespace, podRevision)
	if err != nil {
		return podUpToDate, err
	}
	to, err := r.getControllerRevision(ctx, sts.Namespace, sts.Status.UpdateRevision)
	if err != nil {
		return podUpToDate, err
	}
	if !resourceOnlyRevisionChange(from, to) {
		return podNeedsRestart, nil
	}

	podIdx := containerIndex(pod.Spec.Containers, valkeyContainerName)
	templateIdx := containerIndex(sts.Spec.Template.Spec.Containers, valkeyContainerName)
	if podIdx < 0 || templateIdx < 0 {
		return podNeedsRestart, nil
	}
	desired := podDefaultedResources(sts.Spec.Template.Spec.Containers[templateIdx].Resources)
	current := pod.Spec.Containers[podIdx].Resources
	if !equality.Semantic.DeepEqual(desired.Requests, current.Requests) || !equality.Semantic.DeepEqual(desired.Limits, current.Limits) {
		resized := pod.DeepCopy()
		resized.Spec.Containers[podIdx].Resources.Requests = desired.Requests
		resized.Spec.Containers[podIdx].Resources.Limits = desired.Limits
		if err := r.SubResource("resize").Patch(ctx, resized, client.StrategicMergeFrom(pod)); err != nil {
			if resizeRefused(err) {
				logger.Info("In-place resize refused, restarting the pod instead", "pod", pod.Name, "reason", err.Error())
				return podNeedsRestart, nil
			}
			return podUpToDate, err
		}
		logger.Info("Resized pod in place", "pod", pod.Name,
			"requests", desired.Requests, "limits", desired.Limits)
		pod = resized
	}

	relabelled := pod.DeepCopy()
	relabelled.Labels[appsv1.ControllerRevisionHashLabelKey] = sts.Status.UpdateRevision
	if err := r.Patch(ctx, relabelled, client.MergeFrom(pod)); err != nil {
		return podUpToDate, err
	}
	logger.Info("Moved pod to the StatefulSet's update revision without a restart",
		"pod", pod.Name, "from", podRevision, "to", sts.Status.UpdateRevision)
	return podResizedInPlace, nil
}

// getControllerRevision returns the named ControllerRevision, or nil when it
// does not exist.
func (r *ValkeyClusterReconciler) getControllerRevision(ctx context.Context, namespace, name string) (*appsv1.ControllerRevision, error) {
	if name == "" {
		return nil, nil
	}
	revision := &appsv1.ControllerRevision{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, revision); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return revision, nil
}

// revisionTemplate decodes the pod template a StatefulSet ControllerRevision
// records.
func revisionTemplate(revision *appsv1.ControllerRevision) (corev1.PodTemplateSpec, error) {
	var data struct {
		Spec struct {
			Template corev1.PodTemplateSpec `json:"template"`
		} `json:"spec"`
	}
	err := json.Unmarshal(revision.Data.Raw, &data)
	return data.Spec.Template, err
}

// resourceOnlyRevisionChange reports whether the pod templates of two
// StatefulSet ControllerRevisions differ only in the valkey container's
// resources. A missing or unreadable revision is not a resource-only change.
func resourceOnlyRevisionChange(from, to *appsv1.ControllerRevision) bool {
	if from == nil || to == nil {
		return false
	}
	fromTemplate, err := revisionTemplate(from)
	if err != nil {
		return false
	}
	toTemplate, err := revisionTemplate(to)
	if err != nil {
		return false
	}
	for _, template := range []*corev1.PodTemplateSpec{&fromTemplate, &toTemplate} {
		if i := containerIndex(template.Spec.Containers, valkeyContainerName); i >= 0 {
			template.Spec.Containers[i].Resources = corev1.ResourceRequirements{}
		}
	}
	return equality.Semantic.DeepEqual(fromTemplate, toTemplate)
}

func containerIndex(containers []corev1.Container, name string) int {
	for i := range containers {
		if containers[i].Name == name {
			return i
		}
	}
	return -1
}

// podDefaultedResources applies the API server's pod defaulting to template
// resources: a resource with a limit and no request requests its limit.
func podDefaultedResources(resources corev1.ResourceRequirements) corev1.ResourceRequirements {
	defaulted := *resources.DeepCopy()
	for name, limit := range defaulted.Limits {
		if _, ok := defaulted.Requests[name]; ok {
			continue
		}
		if defaulted.Requests == nil {
			defaulted.Requests = corev1.ResourceList{}
		}
		defaulted.Requests[name] = limit.DeepCopy()
	}
	return defaulted
}

func podResizePending(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == podResizePendingCondition && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// resizeRefused reports whether the API server refused an in-place resize:
// servers older than 1.33 have no resize subresource, and the feature gate,
// QoS class rules, memory limit decreases on 1.33 or RBAC can refuse one.
func resizeRefused(err error) bool {
	return apierrors.IsNotFound(err) || apierrors.IsInvalid(err) || apierrors.IsForbidden(err) ||
		apierrors.IsBadRequest(err) || apierrors.IsMethodNotSupported(err)
}
