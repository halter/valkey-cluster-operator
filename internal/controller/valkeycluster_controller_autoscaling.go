package controller

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cachev1alpha1 "github.com/halter/valkey-cluster-operator/api/v1alpha1"
	internalValkey "github.com/halter/valkey-cluster-operator/internal/controller/valkey"
)

const (
	// valkeyContainerName is the container that runs valkey-server.
	valkeyContainerName = "valkey-cluster-node"

	// resourceScaleUpPercent is the usage, as a share of the current size,
	// above which a resource grows.
	resourceScaleUpPercent = 75

	// resourceTargetPercent is the share of the new size the measured usage
	// fills after a resource is resized.
	resourceTargetPercent = 50

	// resourceScaleDownPercent is the usage, as a share of the current size,
	// below which a resource shrinks.
	resourceScaleDownPercent = 25

	// resourceScaleDownDelay is how long after the last scale a resource has
	// to wait before it shrinks.
	resourceScaleDownDelay = 30 * time.Minute

	// oomGrowthPercent is how much memory grows after an OOM kill.
	oomGrowthPercent = 50

	// cpuSampleMinWindow is the shortest interval a CPU rate is measured over.
	cpuSampleMinWindow = time.Minute
)

// autoscalableResource is a resource the auto-scaler knows how to size.
type autoscalableResource struct {
	name corev1.ResourceName
	// title names the resource at the start of an event message.
	title string
	// step is the multiple that new sizes are rounded up to.
	step resource.Quantity
	// granularity is the multiple that ratio-scaled requests and limits are
	// rounded up to.
	granularity resource.Quantity
}

var autoscalableResources = []autoscalableResource{
	{name: corev1.ResourceMemory, title: "Memory", step: resource.MustParse("64Mi"), granularity: resource.MustParse("1Mi")},
	{name: corev1.ResourceCPU, title: "CPU", step: resource.MustParse("50m"), granularity: resource.MustParse("1m")},
}

// autoscaledResource is a resource a cluster auto-scales, with its bounds and
// current size.
type autoscaledResource struct {
	autoscalableResource
	// floor is the size in spec.resources.
	floor resource.Quantity
	// ceiling is spec.autoscaling.maxResources, raised to floor if lower.
	ceiling resource.Quantity
	// size is status.autoscaledResources clamped to [floor, ceiling], or floor.
	size resource.Quantity
}

// autoscaledResourcesOf returns the resources the cluster auto-scales: those
// named in spec.autoscaling.maxResources that have a size in spec.resources.
func autoscaledResourcesOf(valkeyCluster *cachev1alpha1.ValkeyCluster) []autoscaledResource {
	if valkeyCluster.Spec.Autoscaling == nil {
		return nil
	}
	var resources []autoscaledResource
	for _, r := range autoscalableResources {
		ceiling, ok := valkeyCluster.Spec.Autoscaling.MaxResources[r.name]
		if !ok {
			continue
		}
		floor, ok := specResourceSize(valkeyCluster.Spec.Resources, r.name)
		if !ok {
			continue
		}
		if ceiling.Cmp(floor) < 0 {
			ceiling = floor
		}
		size := floor
		if current, ok := valkeyCluster.Status.AutoscaledResources[r.name]; ok {
			size = clampQuantity(current, floor, ceiling)
		}
		resources = append(resources, autoscaledResource{autoscalableResource: r, floor: floor, ceiling: ceiling, size: size})
	}
	return resources
}

// specResourceSize returns the size of a resource in spec.resources: its
// limit, or its request when it has no limit.
func specResourceSize(resources *corev1.ResourceRequirements, name corev1.ResourceName) (resource.Quantity, bool) {
	if resources == nil {
		return resource.Quantity{}, false
	}
	if limit, ok := resources.Limits[name]; ok && !limit.IsZero() {
		return limit, true
	}
	if request, ok := resources.Requests[name]; ok && !request.IsZero() {
		return request, true
	}
	return resource.Quantity{}, false
}

// effectiveResources returns spec.resources with the request and limit of
// each auto-scaled resource multiplied by its current size over its size in
// spec.resources. Scaling both keeps their ratio, so the pod's QoS class never
// changes, which an in-place resize requires.
func effectiveResources(valkeyCluster *cachev1alpha1.ValkeyCluster) corev1.ResourceRequirements {
	resources := corev1.ResourceRequirements{}
	if valkeyCluster.Spec.Resources != nil {
		resources = *valkeyCluster.Spec.Resources.DeepCopy()
	}
	for _, r := range autoscaledResourcesOf(valkeyCluster) {
		if r.size.Cmp(r.floor) == 0 {
			continue
		}
		for _, list := range []corev1.ResourceList{resources.Requests, resources.Limits} {
			if q, ok := list[r.name]; ok {
				list[r.name] = scaleQuantity(q, r.size, r.floor, r.granularity)
			}
		}
	}
	return resources
}

// scaleQuantity returns q * size / floor rounded up to a multiple of
// granularity.
func scaleQuantity(q, size, floor, granularity resource.Quantity) resource.Quantity {
	num := new(big.Int).Mul(big.NewInt(q.MilliValue()), big.NewInt(size.MilliValue()))
	den := new(big.Int).Mul(big.NewInt(floor.MilliValue()), big.NewInt(granularity.MilliValue()))
	units := new(big.Int).Add(num, den)
	units.Sub(units, big.NewInt(1)).Quo(units, den)
	return *resource.NewMilliQuantity(units.Int64()*granularity.MilliValue(), granularity.Format)
}

func clampQuantity(q, floor, ceiling resource.Quantity) resource.Quantity {
	if q.Cmp(floor) < 0 {
		return floor
	}
	if q.Cmp(ceiling) > 0 {
		return ceiling
	}
	return q
}

// roundUp rounds v up to a multiple of step.
func roundUp(v, step int64) int64 {
	return (v + step - 1) / step * step
}

// resourceScaling is the auto-scaler's decision for one resource.
type resourceScaling struct {
	autoscaledResource
	// to is the new size.
	to resource.Quantity
	// usedPercent is the measured usage as a share of the current size.
	usedPercent int64
	// oomKilled is set on memory when a valkey container was OOM-killed.
	oomKilled bool
	// limited is set when the resource is at its ceiling and needs more.
	limited bool
}

// planResourceScaling sizes each auto-scaled resource from the highest
// per-pod usage (memory in bytes, cpu in millicores) and whether a valkey
// container was OOM-killed since the last scale. It returns the sizes to
// record in status.autoscaledResources and the decision for each resource.
func planResourceScaling(valkeyCluster *cachev1alpha1.ValkeyCluster, usage corev1.ResourceList, oomKilled bool, now time.Time) (corev1.ResourceList, []resourceScaling) {
	last := valkeyCluster.Status.LastResourceScaleTime
	scaleDownAllowed := last == nil || now.Sub(last.Time) >= resourceScaleDownDelay

	resources := autoscaledResourcesOf(valkeyCluster)
	sizes := corev1.ResourceList{}
	plan := make([]resourceScaling, 0, len(resources))
	for _, r := range resources {
		s := resourceScaling{autoscaledResource: r}
		size := r.size.MilliValue()
		want := size
		if q, ok := usage[r.name]; ok {
			used := q.MilliValue()
			s.usedPercent = used * 100 / size
			scaleUp := used*100 > size*resourceScaleUpPercent
			scaleDown := used*100 < size*resourceScaleDownPercent && scaleDownAllowed
			if scaleUp || scaleDown {
				want = roundUp(used*100/resourceTargetPercent, r.step.MilliValue())
			}
		}
		if oomKilled && r.name == corev1.ResourceMemory {
			s.oomKilled = true
			want = max(want, roundUp(size*(100+oomGrowthPercent)/100, r.step.MilliValue()))
		}
		s.limited = want > r.ceiling.MilliValue() && r.size.Cmp(r.ceiling) == 0
		s.to = clampQuantity(*resource.NewMilliQuantity(want, r.step.Format), r.floor, r.ceiling)
		sizes[r.name] = s.to
		plan = append(plan, s)
	}
	return sizes, plan
}

// scaled reports whether usage or an OOM kill changed the resource's size.
func (s resourceScaling) scaled() bool {
	return s.to.Cmp(s.size) != 0
}

// eventMessage describes a usage- or OOM-driven change.
func (s resourceScaling) eventMessage(oomKilledPod string) string {
	switch {
	case s.oomKilled:
		return fmt.Sprintf("%s in pod %s was OOM-killed, scaling %s from %s to %s",
			valkeyContainerName, oomKilledPod, s.name, s.size.String(), s.to.String())
	case s.to.Cmp(s.size) > 0:
		return fmt.Sprintf("%s usage reached %d%% of %s, scaling %s to %s",
			s.title, s.usedPercent, s.size.String(), s.name, s.to.String())
	default:
		return fmt.Sprintf("%s usage fell to %d%% of %s, scaling %s to %s",
			s.title, s.usedPercent, s.size.String(), s.name, s.to.String())
	}
}

// resourcesLimitedCondition returns the ResourcesLimited condition the plan
// calls for, or nil when the condition should stay as it is. The message
// names only the resources and their ceilings, so it does not change as
// usage drifts.
func resourcesLimitedCondition(valkeyCluster *cachev1alpha1.ValkeyCluster, plan []resourceScaling) *metav1.Condition {
	var limited []string
	for _, s := range plan {
		if s.limited {
			limited = append(limited, fmt.Sprintf("%s (%s)", s.name, s.ceiling.String()))
		}
	}
	if len(limited) > 0 {
		return &metav1.Condition{
			Type: typeResourcesLimitedValkeyCluster, Status: metav1.ConditionTrue, Reason: "MaxResourcesReached",
			Message: "Usage needs more than spec.autoscaling.maxResources allows for " + strings.Join(limited, ", "),
		}
	}
	if meta.IsStatusConditionTrue(valkeyCluster.Status.Conditions, typeResourcesLimitedValkeyCluster) {
		return &metav1.Condition{
			Type: typeResourcesLimitedValkeyCluster, Status: metav1.ConditionFalse, Reason: "BelowMaxResources",
			Message: "Usage fits within spec.autoscaling.maxResources",
		}
	}
	return nil
}

// cpuSample is one reading of a valkey process's cumulative CPU time.
type cpuSample struct {
	runID   string
	seconds float64
	at      time.Time
}

// nextCPUSamples pairs each pod's new reading with its previous sample and
// returns the samples to keep for the next measurement and the highest CPU
// rate in millicores, or nil when no pod has a rate yet. A rate needs a
// window of at least cpuSampleMinWindow, so a younger previous sample is kept
// unchanged to let the window grow. A new run_id or a lower counter means the
// process restarted, and the reading becomes the new baseline.
func nextCPUSamples(previous, readings map[types.UID]cpuSample) (map[types.UID]cpuSample, *resource.Quantity) {
	next := make(map[types.UID]cpuSample, len(readings))
	var highest *resource.Quantity
	for uid, reading := range readings {
		prev, ok := previous[uid]
		if !ok || prev.runID != reading.runID || reading.seconds < prev.seconds {
			next[uid] = reading
			continue
		}
		window := reading.at.Sub(prev.at)
		if window < cpuSampleMinWindow {
			next[uid] = prev
			continue
		}
		next[uid] = reading
		millicores := int64(math.Round((reading.seconds - prev.seconds) / window.Seconds() * 1000))
		if highest == nil || millicores > highest.MilliValue() {
			highest = resource.NewMilliQuantity(millicores, resource.DecimalSI)
		}
	}
	return next, highest
}

// oomKilledSince reports whether the pod's valkey container was last
// terminated by an OOM kill after since, or at any time when since is nil.
func oomKilledSince(pod *corev1.Pod, since *metav1.Time) bool {
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name != valkeyContainerName {
			continue
		}
		terminated := status.LastTerminationState.Terminated
		return terminated != nil && terminated.Reason == "OOMKilled" &&
			(since == nil || terminated.FinishedAt.After(since.Time))
	}
	return false
}

func podIsReady(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// reconcileResourceAutoScaling measures the CPU and memory every valkey
// container uses and records new sizes for the auto-scaled resources in
// status. reconcileStatefulSets applies them to the StatefulSet templates and
// reconcilePodRevisions to the pods.
func (r *ValkeyClusterReconciler) reconcileResourceAutoScaling(ctx context.Context, req ctrl.Request, valkeyCluster *cachev1alpha1.ValkeyCluster) (*ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if len(autoscaledResourcesOf(valkeyCluster)) == 0 {
		r.cpuSamples.Delete(req.NamespacedName)
		return nil, r.clearResourceAutoScalingStatus(ctx, req, valkeyCluster)
	}

	podList := &corev1.PodList{}
	if err := r.List(ctx, podList,
		client.InNamespace(valkeyCluster.Namespace),
		client.MatchingLabels(labelsForValkeyCluster(valkeyCluster.Name))); err != nil {
		return nil, err
	}

	now := time.Now()
	usage, oomKilledPod := r.measureResourceUsage(ctx, req.NamespacedName, valkeyCluster, podList.Items, now)
	sizes, plan := planResourceScaling(valkeyCluster, usage, oomKilledPod != "", now)

	if condition := resourcesLimitedCondition(valkeyCluster, plan); condition != nil {
		if err := r.setConditionWithEvent(ctx, req, valkeyCluster, *condition); err != nil {
			return nil, err
		}
	}
	if equality.Semantic.DeepEqual(sizes, valkeyCluster.Status.AutoscaledResources) {
		return nil, nil
	}

	if err := r.Get(ctx, req.NamespacedName, valkeyCluster); err != nil {
		return nil, err
	}
	valkeyCluster.Status.AutoscaledResources = sizes
	var messages []string
	for _, s := range plan {
		if s.scaled() {
			messages = append(messages, s.eventMessage(oomKilledPod))
		}
	}
	if len(messages) > 0 {
		valkeyCluster.Status.LastResourceScaleTime = &metav1.Time{Time: now}
	}
	if err := r.Status().Update(ctx, valkeyCluster); err != nil {
		logger.Error(err, "Failed to update ValkeyCluster status with auto-scaled resources")
		return nil, err
	}
	for _, message := range messages {
		logger.Info(message)
		r.Recorder.Event(valkeyCluster, corev1.EventTypeNormal, "ResourceAutoScale", message)
	}
	return &ctrl.Result{Requeue: true}, nil
}

// measureResourceUsage reads INFO from every ready valkey container and
// returns the highest memory and CPU usage among them, and the name of a pod
// whose valkey container was OOM-killed since the last scale, if any. A pod
// that cannot be measured is skipped.
func (r *ValkeyClusterReconciler) measureResourceUsage(ctx context.Context, key types.NamespacedName, valkeyCluster *cachev1alpha1.ValkeyCluster, pods []corev1.Pod, now time.Time) (corev1.ResourceList, string) {
	logger := log.FromContext(ctx)

	usage := corev1.ResourceList{}
	readings := map[types.UID]cpuSample{}
	oomKilledPod := ""
	for _, pod := range pods {
		if pod.DeletionTimestamp != nil {
			continue
		}
		if oomKilledPod == "" && oomKilledSince(&pod, valkeyCluster.Status.LastResourceScaleTime) {
			oomKilledPod = pod.Name
		}
		if pod.Status.Phase != corev1.PodRunning || !podIsReady(&pod) || pod.Status.PodIP == "" {
			continue
		}
		info, err := r.readResourceInfo(ctx, valkeyCluster, pod.Status.PodIP)
		if err != nil {
			logger.Error(err, "Failed to measure CPU and memory usage", "pod", pod.Name)
			continue
		}
		rss := resource.NewQuantity(info.UsedMemoryRSS, resource.BinarySI)
		if current, ok := usage[corev1.ResourceMemory]; !ok || rss.Cmp(current) > 0 {
			usage[corev1.ResourceMemory] = *rss
		}
		readings[pod.UID] = cpuSample{runID: info.RunID, seconds: info.UsedCPUSeconds, at: now}
	}

	var previous map[types.UID]cpuSample
	if samples, ok := r.cpuSamples.Load(key); ok {
		previous = samples.(map[types.UID]cpuSample)
	}
	next, cpu := nextCPUSamples(previous, readings)
	r.cpuSamples.Store(key, next)
	if cpu != nil {
		usage[corev1.ResourceCPU] = *cpu
	}
	return usage, oomKilledPod
}

func (r *ValkeyClusterReconciler) readResourceInfo(ctx context.Context, valkeyCluster *cachev1alpha1.ValkeyCluster, podIP string) (internalValkey.ResourceInfo, error) {
	valkeyClient, err := r.NewValkeyClient(ctx, valkeyCluster, podIP, VALKEY_PORT)
	if err != nil {
		return internalValkey.ResourceInfo{}, err
	}
	defer valkeyClient.Close()
	infoTxt, err := valkeyClient.Do(ctx, valkeyClient.B().Info().Section("server", "memory", "cpu").Build()).ToString()
	if err != nil {
		return internalValkey.ResourceInfo{}, err
	}
	return internalValkey.ParseInfoResources(infoTxt)
}

// clearResourceAutoScalingStatus removes the auto-scaling status of a cluster
// that auto-scales no resource.
func (r *ValkeyClusterReconciler) clearResourceAutoScalingStatus(ctx context.Context, req ctrl.Request, valkeyCluster *cachev1alpha1.ValkeyCluster) error {
	status := &valkeyCluster.Status
	if len(status.AutoscaledResources) == 0 && status.LastResourceScaleTime == nil &&
		meta.FindStatusCondition(status.Conditions, typeResourcesLimitedValkeyCluster) == nil {
		return nil
	}
	if err := r.Get(ctx, req.NamespacedName, valkeyCluster); err != nil {
		return err
	}
	valkeyCluster.Status.AutoscaledResources = nil
	valkeyCluster.Status.LastResourceScaleTime = nil
	meta.RemoveStatusCondition(&valkeyCluster.Status.Conditions, typeResourcesLimitedValkeyCluster)
	return r.Status().Update(ctx, valkeyCluster)
}
