/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"bytes"
	"context"
	"embed"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/valkey-io/valkey-go"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/tools/remotecommand"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cachev1alpha1 "github.com/halter/valkey-cluster-operator/api/v1alpha1"
	internalValkey "github.com/halter/valkey-cluster-operator/internal/controller/valkey"
)

//go:embed scripts/*
var scripts embed.FS

const valkeyClusterFinalizer = "cache.halter.io/finalizer"

// Definitions to manage status conditions
const (
	// typeAvailableValkeyCluster represents the status of the Statefulset reconciliation
	typeAvailableValkeyCluster = "Available"
	// typeDegradedValkeyCluster represents the status used when the custom resource is deleted and the finalizer operations are yet to occur.
	typeDegradedValkeyCluster = "Degraded"
	// typeReshardingValkeyCluster represents the status used when the custom resource is in the process of resharding.
	typeReshardingValkeyCluster = "Resharding"
	// typeAvailableValkeyCluster represents the status of the Statefulset reconciliation
	typeProvisioningValkeyCluster = "Provisioning"
)

// ValkeyClusterReconciler reconciles a ValkeyCluster object
type ValkeyClusterReconciler struct {
	client.Client
	Scheme     *runtime.Scheme
	Recorder   record.EventRecorder
	RestConfig *rest.Config
	ClientSet  *kubernetes.Clientset
}

// +kubebuilder:rbac:groups=cache.halter.io,resources=valkeyclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cache.halter.io,resources=valkeyclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cache.halter.io,resources=valkeyclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=pods/exec,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the ValkeyCluster object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.19.0/pkg/reconcile
func (r *ValkeyClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	// Fetch the ValkeyCluster instance
	// The purpose is check if the Custom Resource for the Kind ValkeyCluster
	// is applied on the cluster if not we return nil to stop the reconciliation
	valkeyCluster := &cachev1alpha1.ValkeyCluster{}
	err := r.Get(ctx, req.NamespacedName, valkeyCluster)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// If the custom resource is not found then it usually means that it was deleted or not created
			// In this way, we will stop the reconciliation
			log.Info("valkeyCluster resource not found. Ignoring since object must be deleted")
			return ctrl.Result{}, nil
		}
		// Error reading the object - requeue the request.
		log.Error(err, "Failed to get valkeyCluster")
		return ctrl.Result{}, err
	}

	// Let's just set the status as Unknown when no status is available
	if valkeyCluster.Status.Conditions == nil || len(valkeyCluster.Status.Conditions) == 0 {
		meta.SetStatusCondition(&valkeyCluster.Status.Conditions, metav1.Condition{Type: typeAvailableValkeyCluster, Status: metav1.ConditionUnknown, Reason: "Reconciling", Message: "Starting reconciliation"})
		if err = r.Status().Update(ctx, valkeyCluster); err != nil {
			log.Error(err, "Failed to update ValkeyCluster status")
			return ctrl.Result{}, err
		}

		// Let's re-fetch the valkeyCluster Custom Resource after updating the status
		// so that we have the latest state of the resource on the cluster and we will avoid
		// raising the error "the object has been modified, please apply
		// your changes to the latest version and try again" which would re-trigger the reconciliation
		// if we try to update it again in the following operations
		if err := r.Get(ctx, req.NamespacedName, valkeyCluster); err != nil {
			log.Error(err, "Failed to re-fetch valkeyCluster")
			return ctrl.Result{}, err
		}
	}

	// Let's add a finalizer. Then, we can define some operations which should
	// occur before the custom resource is deleted.
	// More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/finalizers
	if !controllerutil.ContainsFinalizer(valkeyCluster, valkeyClusterFinalizer) {
		log.Info("Adding Finalizer for ValkeyCluster")
		if ok := controllerutil.AddFinalizer(valkeyCluster, valkeyClusterFinalizer); !ok {
			log.Error(err, "Failed to add finalizer into the custom resource")
			return ctrl.Result{Requeue: true}, nil
		}

		if err = r.Update(ctx, valkeyCluster); err != nil {
			log.Error(err, "Failed to update custom resource to add finalizer")
			return ctrl.Result{}, err
		}
	}

	// Check if the ValkeyCluster instance is marked to be deleted, which is
	// indicated by the deletion timestamp being set.
	isValkeyClusterMarkedToBeDeleted := valkeyCluster.GetDeletionTimestamp() != nil
	if isValkeyClusterMarkedToBeDeleted {
		if controllerutil.ContainsFinalizer(valkeyCluster, valkeyClusterFinalizer) {
			log.Info("Performing Finalizer Operations for ValkeyCluster before delete CR")

			// Let's add here a status "Downgrade" to reflect that this resource began its process to be terminated.
			meta.SetStatusCondition(&valkeyCluster.Status.Conditions, metav1.Condition{Type: typeDegradedValkeyCluster,
				Status: metav1.ConditionUnknown, Reason: "Finalizing",
				Message: fmt.Sprintf("Performing finalizer operations for the custom resource: %s ", valkeyCluster.Name)})

			if err := r.Status().Update(ctx, valkeyCluster); err != nil {
				log.Error(err, "Failed to update ValkeyCluster status")
				return ctrl.Result{}, err
			}

			// Perform all operations required before removing the finalizer and allow
			// the Kubernetes API to remove the custom resource.
			r.doFinalizerOperationsForValkeyCluster(valkeyCluster)

			// TODO(user): If you add operations to the doFinalizerOperationsForValkeyCluster method
			// then you need to ensure that all worked fine before deleting and updating the Downgrade status
			// otherwise, you should requeue here.

			// Re-fetch the valkeyCluster Custom Resource before updating the status
			// so that we have the latest state of the resource on the cluster and we will avoid
			// raising the error "the object has been modified, please apply
			// your changes to the latest version and try again" which would re-trigger the reconciliation
			if err := r.Get(ctx, req.NamespacedName, valkeyCluster); err != nil {
				log.Error(err, "Failed to re-fetch valkeyCluster")
				return ctrl.Result{}, err
			}

			meta.SetStatusCondition(&valkeyCluster.Status.Conditions, metav1.Condition{Type: typeDegradedValkeyCluster,
				Status: metav1.ConditionTrue, Reason: "Finalizing",
				Message: fmt.Sprintf("Finalizer operations for custom resource %s name were successfully accomplished", valkeyCluster.Name)})

			if err := r.Status().Update(ctx, valkeyCluster); err != nil {
				log.Error(err, "Failed to update ValkeyCluster status")
				return ctrl.Result{}, err
			}

			log.Info("Removing Finalizer for ValkeyCluster after successfully perform the operations")
			if ok := controllerutil.RemoveFinalizer(valkeyCluster, valkeyClusterFinalizer); !ok {
				log.Error(err, "Failed to remove finalizer for ValkeyCluster")
				return ctrl.Result{Requeue: true}, nil
			}

			if err := r.Update(ctx, valkeyCluster); err != nil {
				log.Error(err, "Failed to remove finalizer for ValkeyCluster")
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	err = r.upsertConfigMap(ctx, valkeyCluster)
	if err != nil {
		log.Error(err, "Failed to upsert configmap")
		return ctrl.Result{}, err
	}

	err = r.upsertHeadlessService(ctx, valkeyCluster)
	if err != nil {
		log.Error(err, "Failed to upsert Service")
		return ctrl.Result{}, err
	}

	// Check if the statefulset already exists, if not create a new one
	for stsIdx := 0; stsIdx < int(valkeyCluster.Spec.Shards); stsIdx++ {
		found := &appsv1.StatefulSet{}
		stsName := fmt.Sprintf("%s-%d", valkeyCluster.Name, stsIdx)
		err = r.Get(ctx, types.NamespacedName{Name: stsName, Namespace: valkeyCluster.Namespace}, found)
		if err != nil && apierrors.IsNotFound(err) {
			log.Info(fmt.Sprintf("StatefulSet %s not found", stsName))
			// Define a new statefulset
			sts := r.statefulSet(stsName, statefulSetSize(valkeyCluster), valkeyCluster)
			// Set the ownerRef for the StatefulSet
			// More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/owners-dependents/
			if err := ctrl.SetControllerReference(valkeyCluster, sts, r.Scheme); err != nil {
				log.Error(err, "Failed to define new StatefulSet resource for ValkeyCluster")

				// The following implementation will update the status
				meta.SetStatusCondition(&valkeyCluster.Status.Conditions, metav1.Condition{Type: typeAvailableValkeyCluster,
					Status: metav1.ConditionFalse, Reason: "Reconciling",
					Message: fmt.Sprintf("Failed to create StatefulSet for the custom resource (%s): (%s)", valkeyCluster.Name, err)})

				if err := r.Status().Update(ctx, valkeyCluster); err != nil {
					log.Error(err, "Failed to update ValkeyCluster status")
					return ctrl.Result{}, err
				}

				return ctrl.Result{}, err
			}

			log.Info("Creating a new StatefulSet",
				"StatefulSet.Namespace", sts.Namespace, "StatefulSet.Name", sts.Name)
			if err = r.Create(ctx, sts); err != nil {
				log.Error(err, "Failed to create new StatefulSet",
					"StatefulSet.Namespace", sts.Namespace, "StatefulSet.Name", sts.Name)
				return ctrl.Result{}, err
			}
			r.Recorder.Event(valkeyCluster, "Normal", "Created",
				fmt.Sprintf("StatefulSet %s/%s is created", valkeyCluster.Namespace, sts.Name))

			// StatefulSet created successfully
			// We will requeue the reconciliation so that we can ensure the state
			// and move forward for the next operations
			return ctrl.Result{RequeueAfter: time.Minute}, nil
		} else if err != nil {
			log.Error(err, "Failed to get StatefulSet")
			// Let's return the error for the reconciliation be re-trigged again
			return ctrl.Result{}, err
		}

		// check if any update is occuring for stateful set, if so re-schedule reconcile
		if found.Status.CurrentRevision != found.Status.UpdateRevision {
			return ctrl.Result{RequeueAfter: time.Minute}, nil
		}

		// update containers[0].Command if there is a difference
		// Update if there is a difference in the following attributes:
		// Command
		// Lifecycle
		// Env
		desired := r.statefulSet(stsName, statefulSetSize(valkeyCluster), valkeyCluster)
		if !(cmp.Equal(found.Spec.Template.Spec.Containers[0].Command, desired.Spec.Template.Spec.Containers[0].Command) &&
			cmp.Equal(found.Spec.Template.Spec.Containers[0].Lifecycle, desired.Spec.Template.Spec.Containers[0].Lifecycle) &&
			cmp.Equal(found.Spec.Template.Spec.Containers[0].Env, desired.Spec.Template.Spec.Containers[0].Env)) {

			if !cmp.Equal(found.Spec.Template.Spec.Containers[0].Command, desired.Spec.Template.Spec.Containers[0].Command) {
				log.Info(fmt.Sprintf("StatefulSet %s Command is different: %s", stsName, cmp.Diff(found.Spec.Template.Spec.Containers[0].Command, desired.Spec.Template.Spec.Containers[0].Command)))
			}
			if !cmp.Equal(found.Spec.Template.Spec.Containers[0].Lifecycle, desired.Spec.Template.Spec.Containers[0].Lifecycle) {
				log.Info(fmt.Sprintf("StatefulSet %s Lifecycle is different: %s", stsName, cmp.Diff(found.Spec.Template.Spec.Containers[0].Lifecycle, desired.Spec.Template.Spec.Containers[0].Lifecycle)))
			}
			if !cmp.Equal(found.Spec.Template.Spec.Containers[0].Env, desired.Spec.Template.Spec.Containers[0].Env) {
				log.Info(fmt.Sprintf("StatefulSet %s Env is different: %s", stsName, cmp.Diff(found.Spec.Template.Spec.Containers[0].Env, desired.Spec.Template.Spec.Containers[0].Env)))
			}

			found := &appsv1.StatefulSet{}
			err = r.Get(ctx, types.NamespacedName{Name: stsName, Namespace: valkeyCluster.Namespace}, found)
			if err != nil {
				log.Error(err, "Failed to get StatefulSet")
				// Let's return the error for the reconciliation be re-trigged again
				return ctrl.Result{}, err
			}
			found.Spec.Template.Spec.Containers[0].Command = desired.Spec.Template.Spec.Containers[0].Command
			found.Spec.Template.Spec.Containers[0].Lifecycle = desired.Spec.Template.Spec.Containers[0].Lifecycle
			found.Spec.Template.Spec.Containers[0].Env = desired.Spec.Template.Spec.Containers[0].Env

			if err = r.Update(ctx, found); err != nil {
				log.Error(err, "Failed to update StatefulSet",
					"StatefulSet.Namespace", found.Namespace, "StatefulSet.Name", found.Name)

				if err := r.Get(ctx, req.NamespacedName, valkeyCluster); err != nil {
					log.Error(err, "Failed to re-fetch valkeyCluster")
					return ctrl.Result{}, err
				}

				meta.SetStatusCondition(&valkeyCluster.Status.Conditions, metav1.Condition{Type: typeAvailableValkeyCluster,
					Status: metav1.ConditionFalse, Reason: "Reconciling",
					Message: fmt.Sprintf("Failed to update StatefulSet containers (%s): (%s)", valkeyCluster.Name, err)})

				if err := r.Status().Update(ctx, valkeyCluster); err != nil {
					log.Error(err, "Failed to update ValkeyCluster status")
					return ctrl.Result{}, err
				}

				return ctrl.Result{}, err
			}

			r.Recorder.Event(valkeyCluster, "Normal", "Updated",
				fmt.Sprintf("StatefulSet Containers are updated for %s/%s", found.Namespace, found.Name))

			return ctrl.Result{RequeueAfter: time.Minute}, nil
		}

		// We can simply change the number of replicas inside the shard as that has no impact on data availabilty
		if *found.Spec.Replicas != (statefulSetSize(valkeyCluster)) && *found.Spec.Replicas < (statefulSetSize(valkeyCluster)) {
			log.Info(fmt.Sprintf("StatefulSet needs to increase replicas from %d to %d", *found.Spec.Replicas, (valkeyCluster.Spec.Shards + valkeyCluster.Spec.Replicas)))
			found.Spec.Replicas = &[]int32{(statefulSetSize(valkeyCluster))}[0]
			if err = r.Update(ctx, found); err != nil {
				log.Error(err, "Failed to update StatefulSet",
					"StatefulSet.Namespace", found.Namespace, "StatefulSet.Name", found.Name)

				// Re-fetch the valkeyCluster Custom Resource before updating the status
				// so that we have the latest state of the resource on the cluster and we will avoid
				// raising the error "the object has been modified, please apply
				// your changes to the latest version and try again" which would re-trigger the reconciliation
				if err := r.Get(ctx, req.NamespacedName, valkeyCluster); err != nil {
					log.Error(err, "Failed to re-fetch valkeyCluster")
					return ctrl.Result{}, err
				}

				// The following implementation will update the status
				meta.SetStatusCondition(&valkeyCluster.Status.Conditions, metav1.Condition{Type: typeAvailableValkeyCluster,
					Status: metav1.ConditionFalse, Reason: "Resizing",
					Message: fmt.Sprintf("Failed to update the size for the custom resource (%s): (%s)", valkeyCluster.Name, err)})

				if err := r.Status().Update(ctx, valkeyCluster); err != nil {
					log.Error(err, "Failed to update ValkeyCluster status")
					return ctrl.Result{}, err
				}

				return ctrl.Result{}, err
			}

			log.Info("StatefulSet replicas updated")

			// Now, that we update the size we want to requeue the reconciliation
			// so that we can ensure that we have the latest state of the resource before
			// update. Also, it will help ensure the desired state on the cluster
			return ctrl.Result{Requeue: true}, nil
		}

		foundResources := found.Spec.Template.Spec.Containers[0].Resources
		log.Info(fmt.Sprintf("StatefulSet resources: %v", foundResources))

		foundRequests := foundResources.Requests
		if foundRequests == nil {
			found.Spec.Template.Spec.Containers[0].Resources.Requests = valkeyCluster.Spec.Resources.Requests
			err := r.Update(ctx, found)
			if err != nil {
				log.Error(err, "Failed to update ValkeyCluster resource requests")
				return ctrl.Result{}, err
			}
			r.Recorder.Event(valkeyCluster, "Normal", "Updated",
				fmt.Sprintf("StatefulSet resources requests %s/%s is updated", found.Namespace, found.Name))
			return ctrl.Result{Requeue: true}, nil
		} else {
			// scaling up
			if foundRequests.Cpu().Cmp(*valkeyCluster.Spec.Resources.Requests.Cpu()) == -1 {
				found.Spec.Template.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU] = *valkeyCluster.Spec.Resources.Requests.Cpu()
				err := r.Update(ctx, found)
				if err != nil {
					log.Error(err, "Failed to update ValkeyCluster resources")
					return ctrl.Result{}, err
				}
				r.Recorder.Event(valkeyCluster, "Normal", "Updated",
					fmt.Sprintf("StatefulSet CPU requests %s/%s is updated", found.Namespace, found.Name))
				return ctrl.Result{Requeue: true}, nil
			}
			// scaling up
			if foundRequests.Memory().Cmp(*valkeyCluster.Spec.Resources.Requests.Memory()) == -1 {
				found.Spec.Template.Spec.Containers[0].Resources.Requests[corev1.ResourceMemory] = *valkeyCluster.Spec.Resources.Requests.Memory()
				err := r.Update(ctx, found)
				if err != nil {
					log.Error(err, "Failed to update ValkeyCluster resources")
					return ctrl.Result{}, err
				}
				r.Recorder.Event(valkeyCluster, "Normal", "Updated",
					fmt.Sprintf("StatefulSet Memory requests %s/%s is updated", found.Namespace, found.Name))
				return ctrl.Result{Requeue: true}, nil
			}
		}

		if found.Spec.MinReadySeconds != valkeyCluster.Spec.MinReadySeconds {
			log.Info(fmt.Sprintf("StatefulSet needs to change minReadySeconds from %d to %d", found.Spec.MinReadySeconds, valkeyCluster.Spec.MinReadySeconds))
			found.Spec.MinReadySeconds = valkeyCluster.Spec.MinReadySeconds
			if err := r.Update(ctx, found); err != nil {
				log.Error(err, "Failed to update ValkeyCluster minReadySeconds")
				return ctrl.Result{}, err
			}
			r.Recorder.Event(valkeyCluster, "Normal", "Updated",
				fmt.Sprintf("StatefulSet MinReadySeconds %s/%s is updated", found.Namespace, found.Name))

			log.Info("StatefulSet minReadySeconds updated")

			return ctrl.Result{Requeue: true}, nil
		}

		foundLimits := foundResources.Limits
		if foundLimits == nil {
			found.Spec.Template.Spec.Containers[0].Resources.Limits = valkeyCluster.Spec.Resources.Limits
			err := r.Update(ctx, found)
			if err != nil {
				log.Error(err, "Failed to update ValkeyCluster resource limits")
				return ctrl.Result{}, err
			}
			r.Recorder.Event(valkeyCluster, "Normal", "Updated",
				fmt.Sprintf("StatefulSet resources limits %s/%s is updated", found.Namespace, found.Name))
			return ctrl.Result{Requeue: true}, nil
		} else {
			// scaling up
			if foundLimits.Cpu().Cmp(*valkeyCluster.Spec.Resources.Limits.Cpu()) == -1 {
				found.Spec.Template.Spec.Containers[0].Resources.Limits[corev1.ResourceCPU] = *valkeyCluster.Spec.Resources.Limits.Cpu()
				err := r.Update(ctx, found)
				if err != nil {
					log.Error(err, "Failed to update ValkeyCluster resources")
					return ctrl.Result{}, err
				}
				r.Recorder.Event(valkeyCluster, "Normal", "Updated",
					fmt.Sprintf("StatefulSet CPU limits %s/%s is updated", found.Namespace, found.Name))
				return ctrl.Result{Requeue: true}, nil
			}
			// scaling up
			if foundLimits.Memory().Cmp(*valkeyCluster.Spec.Resources.Limits.Memory()) == -1 {
				found.Spec.Template.Spec.Containers[0].Resources.Limits[corev1.ResourceMemory] = *valkeyCluster.Spec.Resources.Limits.Memory()
				err := r.Update(ctx, found)
				if err != nil {
					log.Error(err, "Failed to update ValkeyCluster resources")
					return ctrl.Result{}, err
				}
				r.Recorder.Event(valkeyCluster, "Normal", "Updated",
					fmt.Sprintf("StatefulSet Memory limits %s/%s is updated", found.Namespace, found.Name))
				return ctrl.Result{Requeue: true}, nil
			}
		}

	}

	// wait for all pods to be running and accessible via valkey-client
	res, err := r.waitForPodsToBeAccessibleViaValkey(ctx, valkeyCluster)
	if err != nil {
		log.Error(err, "Failed to list pods", "ValkeyCluster.Namespace", valkeyCluster.Namespace, "ValkeyCluster.Name", valkeyCluster.Name)
		return ctrl.Result{}, err
	}
	if res != nil {
		// requeue request from wait function
		return *res, nil
	}

	// Check if we need to remove a shard
	stsList := &appsv1.StatefulSetList{}
	listOpts := []client.ListOption{
		client.InNamespace(valkeyCluster.Namespace),
		client.MatchingLabels(labelsForValkeyCluster(valkeyCluster.Name)),
	}
	if err = r.List(ctx, stsList, listOpts...); err != nil {
		log.Error(err, "Failed to list StatefulSets", "ValkeyCluster.Namespace", valkeyCluster.Namespace, "ValkeyCluster.Name", valkeyCluster.Name)
		return ctrl.Result{}, err
	}
	// we need to scale down
	if len(stsList.Items) > 1 && int(valkeyCluster.Spec.Shards) < len(stsList.Items) {
		// first we need to check if the nth shard has all it's slots re-allocated
		lastIdx := 0
		var lastSts appsv1.StatefulSet
		re := regexp.MustCompile(fmt.Sprintf(`%s-(\d+)`, valkeyCluster.Name))
		for _, sts := range stsList.Items {
			matches := re.FindAllStringSubmatch(sts.Name, -1)
			stsIdx, err := strconv.Atoi(matches[0][1])
			if err != nil {
				log.Error(err, "Failed to get StatefulSet index from", "StatefulSet.name", sts.Name)
				return ctrl.Result{}, err
			}
			if stsIdx > lastIdx {
				lastIdx = stsIdx
				lastSts = sts
			}
		}

		clusterNodes, err := r.buildClusterNodes(ctx, valkeyCluster)
		if err != nil {
			log.Error(err, "Failed to build cluster nodes")
			return ctrl.Result{}, err
		}

		clusterNodesInLastSts := make([]*internalValkey.ClusterNode, 0)
		slotsInShard := false
		for _, cn := range clusterNodes {
			if !strings.HasPrefix(cn.Pod, fmt.Sprintf("%s-%d-", valkeyCluster.Name, lastIdx)) {
				continue
			}
			clusterNodesInLastSts = append(clusterNodesInLastSts, cn)
			if cn.HasSlots() {
				slotsInShard = true
			}
		}
		if !slotsInShard {
			// first we need to delete all node from cluster
			for _, cn := range clusterNodesInLastSts {
				log.Info(fmt.Sprintf("Removing %s from %s/%s", cn.ID, valkeyCluster.Namespace, valkeyCluster.Name))
				_, _, err := r.executeValkeyCli(ctx, valkeyCluster, []string{"--cluster", "del-node", "127.0.0.1:6379", cn.ID})
				if err != nil {
					log.Error(err, "Failed to remove cluster node")
					return ctrl.Result{}, err
				}
			}

			if err := r.Delete(ctx, &lastSts); err != nil {
				log.Error(err, "Failed to delete StatfulSet", "StatefulSet", lastSts)
				return ctrl.Result{}, err
			}
			r.Recorder.Event(valkeyCluster, "Normal", "Deleted",
				fmt.Sprintf("StatefulSet %s/%s is deleted", lastSts.Namespace, lastSts.Name))
		}
	}

	// check if pvs already exist, they should be created by the statefulset
	for stsIdx := 0; stsIdx < int(valkeyCluster.Spec.Shards); stsIdx++ {
		for pvcIdx := 0; pvcIdx < int(statefulSetSize(valkeyCluster)); pvcIdx++ {
			pvcName := fmt.Sprintf("valkey-data-%s-%d-%d", valkeyCluster.Name, stsIdx, pvcIdx)
			found := &corev1.PersistentVolumeClaim{}
			err = r.Get(ctx, types.NamespacedName{Name: pvcName, Namespace: valkeyCluster.Namespace}, found)
			if err != nil && apierrors.IsNotFound(err) {
				log.Info("PVC not found, requeuing", "PVC", pvcName)
				return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
			} else if err != nil {
				log.Error(err, "Failed to get PersistentVolumeClaims")
				// Let's return the error for the reconciliation be re-trigged again
				return ctrl.Result{}, err
			}

			if found.Spec.Resources.Requests.Storage().Cmp(*valkeyCluster.Spec.Storage.Resources.Requests.Storage()) == -1 {
				found.Spec.Resources.Requests[corev1.ResourceStorage] = *valkeyCluster.Spec.Storage.Resources.Requests.Storage()
				if err = r.Update(ctx, found); err != nil {
					log.Error(err, "Failed to update PersistentVolumeClaim",
						"PersistentVolumeClaim.Namespace", found.Namespace, "PersistentVolumeClaim.Name", found.Name)

					// Re-fetch the valkeyCluster Custom Resource before updating the status
					// so that we have the latest state of the resource on the cluster and we will avoid
					// raising the error "the object has been modified, please apply
					// your changes to the latest version and try again" which would re-trigger the reconciliation
					if err := r.Get(ctx, req.NamespacedName, valkeyCluster); err != nil {
						log.Error(err, "Failed to re-fetch valkeyCluster")
						return ctrl.Result{}, err
					}

					// The following implementation will update the status
					meta.SetStatusCondition(&valkeyCluster.Status.Conditions, metav1.Condition{Type: typeAvailableValkeyCluster,
						Status: metav1.ConditionFalse, Reason: "Resizing",
						Message: fmt.Sprintf("Failed to update the size for the custom resource (%s): (%s)", valkeyCluster.Name, err)})

					if err := r.Status().Update(ctx, valkeyCluster); err != nil {
						log.Error(err, "Failed to update ValkeyCluster status")
						return ctrl.Result{}, err
					}
					return ctrl.Result{}, err
				}
				r.Recorder.Event(valkeyCluster, "Normal", "Updated",
					fmt.Sprintf("PersistentVolumeClaim %s/%s is updated", found.Namespace, found.Name))
				return ctrl.Result{Requeue: true}, nil
			}
		}
	}

	clusterNodes, err := r.buildClusterNodes(ctx, valkeyCluster)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("Could not build cluster nodes: %v", err)
	}

	// cluster meet
	for _, clusterNodeA := range clusterNodes {
		for _, clusterNodeB := range clusterNodes {
			if clusterNodeA.Pod != clusterNodeB.Pod {
				client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{clusterNodeA.IP + ":6379"}, ForceSingleClient: true})
				if err != nil {
					log.Error(err, "Failed to create Valkey client")
					return ctrl.Result{}, err
				}
				defer client.Close()

				txt, err := client.Do(ctx, client.B().ClusterNodes().Build()).ToString()
				if err != nil {
					log.Error(err, "Failed to get cluster nodes")
					return ctrl.Result{}, err
				}
				clusterNodes, err := internalValkey.ParseClusterNodesExludeSelf(txt)
				if err != nil {
					log.Error(err, "Failed to parse cluster nodes")
					return ctrl.Result{}, err
				}
				met := false
				for _, cn := range clusterNodes {
					if cn.ID == clusterNodeB.ID {
						met = true
					}
				}
				if met {
					continue
				}

				err = client.Do(ctx, client.B().ClusterMeet().Ip(clusterNodeB.IP).Port(6379).Build()).Error()
				if err != nil {
					log.Error(err, "Failed to do cluster meet")
					return ctrl.Result{}, err
				}

				log.Info("Cluster nodes", "ClusterNodes", clusterNodes)
				clusterNodes, err = r.buildClusterNodes(ctx, valkeyCluster)
				if err != nil {
					log.Error(err, "Failed to build cluster nodes")
					return ctrl.Result{}, err
				}

				err = r.updateClusterNodesStatus(ctx, req)
				if err != nil {
					log.Error(err, "Failed to update ValkeyCluster status")
					return ctrl.Result{}, err
				}

				meta.SetStatusCondition(&valkeyCluster.Status.Conditions, metav1.Condition{Type: typeAvailableValkeyCluster,
					Status: metav1.ConditionFalse, Reason: "Reconciling",
					Message: fmt.Sprintf("Ran cluster meet operation on %s with %d pods", valkeyCluster.Name, len(clusterNodes))})

				if err := r.Status().Update(ctx, valkeyCluster); err != nil {
					log.Error(err, "Failed to update ValkeyCluster status")
					return ctrl.Result{}, err
				}
			}
		}
	}

	valkeyCluster = &cachev1alpha1.ValkeyCluster{}
	err = r.Get(ctx, req.NamespacedName, valkeyCluster)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// If the custom resource is not found then it usually means that it was deleted or not created
			// In this way, we will stop the reconciliation
			log.Info("valkeyCluster resource not found. Ignoring since object must be deleted")
			return ctrl.Result{}, nil
		}
		// Error reading the object - requeue the request.
		log.Error(err, "Failed to get valkeyCluster")
		return ctrl.Result{}, err
	}

	clusterNodes, err = r.buildClusterNodes(ctx, valkeyCluster)
	if err != nil {
		log.Error(err, "Failed to build cluster nodes")
		return ctrl.Result{}, err
	}
	err = r.updateClusterNodesStatus(ctx, req)
	if err != nil {
		log.Error(err, "Failed to update ValkeyCluster status")
		return ctrl.Result{}, err
	}

	clusterNodes, err = r.buildClusterNodes(ctx, valkeyCluster)
	if err != nil {
		log.Error(err, "Failed to get cluster nodes")
		return ctrl.Result{}, err
	}

	// There are 16384 hash slots in Valkey Cluster, and to compute the hash slot for a given key, we simply take the CRC16 of the key modulo 16384.
	// 0-16383

	// first check if any slots have already been assigned
	foundExistingSlots := false
	for _, cn := range clusterNodes {
		if cn.HasSlots() {
			foundExistingSlots = true
		}
	}

	if !foundExistingSlots {
		slotRanges := internalValkey.SlotRanges(int(valkeyCluster.Spec.Shards))
		for shardIdx := 0; shardIdx < int(valkeyCluster.Spec.Shards); shardIdx++ {
			clusterNodesForShard := make([]*internalValkey.ClusterNode, 0)
			for _, cn := range clusterNodes {
				if strings.HasPrefix(cn.Pod, fmt.Sprintf("%s-%d-", valkeyCluster.Name, shardIdx)) {
					clusterNodesForShard = append(clusterNodesForShard, cn)
				}
			}

			expectedSlotRanges := slotRanges[shardIdx]
			log.Info(fmt.Sprintf("Expected slot range %+v for shard %d not found", expectedSlotRanges, shardIdx))
			client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{clusterNodesForShard[0].IP + ":6379"}, ForceSingleClient: true})
			if err != nil {
				log.Error(err, "Failed to get client")
				return ctrl.Result{}, err
			}
			err = client.Do(ctx, client.B().ClusterAddslotsrange().StartSlotEndSlot().StartSlotEndSlot(int64(expectedSlotRanges.Start), int64(expectedSlotRanges.End)).Build()).Error()
			if err != nil {
				log.Error(err, "Failed to add slot range")
				return ctrl.Result{}, err
			}
		}
	}

	// setup replication
	clusterNodes, err = r.buildClusterNodes(ctx, valkeyCluster)
	if err != nil {
		log.Error(err, "Failed to get cluster nodes")
		return ctrl.Result{}, err
	}
	for shardIdx := 0; shardIdx < int(valkeyCluster.Spec.Shards); shardIdx++ {
		clusterNodesForShard := make([]*internalValkey.ClusterNode, 0)
		for _, cn := range clusterNodes {
			if strings.HasPrefix(cn.Pod, fmt.Sprintf("%s-%d-", valkeyCluster.Name, shardIdx)) {
				clusterNodesForShard = append(clusterNodesForShard, cn)
			}
		}

		var primary *internalValkey.ClusterNode
		for _, cn := range clusterNodesForShard {
			if cn.HasSlots() {
				primary = cn

			}
		}
		if primary != nil {
			for _, cn := range clusterNodesForShard {
				if cn.Pod != primary.Pod {
					if cn.MasterNodeID == primary.ID {
						continue
					}
					client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{cn.IP + ":6379"}, ForceSingleClient: true})
					if err != nil {
						log.Error(err, "Failed to get client")
						return ctrl.Result{}, err
					}
					err = client.Do(ctx, client.B().ClusterReplicate().NodeId(primary.ID).Build()).Error()
					if err != nil {
						log.Error(err, "Failed to setup replication")
						return ctrl.Result{}, err
					}
				}
			}
		} else {
			// Pick the master node to set all other nodes to replicate from
			masterCount := 0
			for _, cn := range clusterNodesForShard {
				if cn.IsMaster() {
					masterCount++
					primary = cn
				}
			}
			if masterCount == 0 {
				err := fmt.Errorf("%s/%s shard %d There are no master nodes in the shard", valkeyCluster.Namespace, valkeyCluster.Name, shardIdx)
				log.Error(err, "No masters in shard, invalid state")
				return ctrl.Result{}, err
			}

			for _, cn := range clusterNodesForShard {
				if cn.ID == primary.ID {
					continue
				}
				client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{cn.IP + ":6379"}, ForceSingleClient: true})
				if err != nil {
					log.Error(err, "Failed to get client")
					return ctrl.Result{}, err
				}
				err = client.Do(ctx, client.B().ClusterReplicate().NodeId(primary.ID).Build()).Error()
				if err != nil {
					log.Error(err, "Failed to setup replication")
					return ctrl.Result{}, err
				}
			}
		}
	}

	// RESHARDING
	res, err = r.reconcileValkeySlots(ctx, valkeyCluster)
	if err != nil {
		log.Error(err, "Failed to reshard")
		return ctrl.Result{}, err
	}
	if res != nil {
		return *res, nil
	}

	// Check if we need to remove a PVCs
	pvcList := &corev1.PersistentVolumeClaimList{}
	listOpts = []client.ListOption{
		client.InNamespace(valkeyCluster.Namespace),
		client.MatchingLabels(labelsForValkeyCluster(valkeyCluster.Name)),
	}
	if err = r.List(ctx, pvcList, listOpts...); err != nil {
		log.Error(err, "Failed to list PersistentVolumeClaims", "ValkeyCluster.Namespace", valkeyCluster.Namespace, "ValkeyCluster.Name", valkeyCluster.Name)
		return ctrl.Result{}, err
	}
	if len(pvcList.Items) > int(valkeyCluster.Spec.Shards+(valkeyCluster.Spec.Shards*valkeyCluster.Spec.Replicas)) {
		// ok we need to delete some PVCs but only if the corresponding StatefulSet does not exist
		stsList := &appsv1.StatefulSetList{}
		listOpts := []client.ListOption{
			client.InNamespace(valkeyCluster.Namespace),
			client.MatchingLabels(labelsForValkeyCluster(valkeyCluster.Name)),
		}
		if err = r.List(ctx, stsList, listOpts...); err != nil {
			log.Error(err, "Failed to list StatefulSets", "ValkeyCluster.Namespace", valkeyCluster.Namespace, "ValkeyCluster.Name", valkeyCluster.Name)
			return ctrl.Result{}, err
		}

		pvcsToDelete := []corev1.PersistentVolumeClaim{}
		for _, pvc := range pvcList.Items {
			// check if this PVC must be deleted
			mustDelete := true
			for _, sts := range stsList.Items {
				if strings.HasPrefix(pvc.Name, fmt.Sprintf("valkey-data-%s-", sts.Name)) {
					mustDelete = false
				}
			}
			if mustDelete {
				pvcsToDelete = append(pvcsToDelete, pvc)
			}
		}

		// we also want to delete pvcs if the number of replicas has gone down
		for _, sts := range stsList.Items {
			pvcsAssocatedWithSts := []corev1.PersistentVolumeClaim{}
			for _, pvc := range pvcList.Items {
				if strings.HasPrefix(pvc.Name, fmt.Sprintf("valkey-data-%s-", sts.Name)) {
					pvcsAssocatedWithSts = append(pvcsAssocatedWithSts, pvc)
				}
			}

			// too many PVCs we need to delete the ones that do not have a pod associated
			if len(pvcsAssocatedWithSts) > int(*sts.Spec.Replicas) {
				podList := &corev1.PodList{}
				listOpts := []client.ListOption{
					client.InNamespace(valkeyCluster.Namespace),
					client.MatchingLabels(labelsForValkeyCluster(valkeyCluster.Name)),
				}
				if err := r.List(ctx, podList, listOpts...); err != nil {
					log.Error(err, "Failed to list pods", "ValkeyCluster.Namespace", valkeyCluster.Namespace, "ValkeyCluster.Name", valkeyCluster.Name)
					return ctrl.Result{}, err
				}
				for _, pvc := range pvcsAssocatedWithSts {
					hasPod := false
					for _, pod := range podList.Items {
						if pvc.Name == fmt.Sprintf("valkey-data-%s", pod.Name) {
							hasPod = true
						}
					}

					if !hasPod {
						pvcsToDelete = append(pvcsToDelete, pvc)
					}
				}
			}
		}

		// ensure list is unique
		tmp := make(map[string]corev1.PersistentVolumeClaim)
		for _, toDelete := range pvcsToDelete {
			tmp[toDelete.Name] = toDelete
		}
		pvcsToDelete = nil
		for _, toDelete := range tmp {
			pvcsToDelete = append(pvcsToDelete, toDelete)
		}

		if len(pvcList.Items)-len(pvcsToDelete) != int(valkeyCluster.Spec.Shards+(valkeyCluster.Spec.Shards*valkeyCluster.Spec.Replicas)) {
			err := fmt.Errorf("Expected resulting number of PVC to match the number of nodes %d but got %d", int(valkeyCluster.Spec.Shards+(valkeyCluster.Spec.Shards*valkeyCluster.Spec.Replicas)), len(pvcList.Items)-len(pvcsToDelete))
			return ctrl.Result{}, err
		}

		for _, toDelete := range pvcsToDelete {
			if err := r.Delete(ctx, &toDelete); err != nil {
				log.Error(err, "Failed to delete PVC", "ValkeyCluster.Namespace", valkeyCluster.Namespace, "PVC.Name", toDelete.Name)
				return ctrl.Result{}, err
			}
			r.Recorder.Event(valkeyCluster, "Normal", "Deleted PVC",
				fmt.Sprintf("PVC %s deleted", toDelete.Name))
		}
	}

	// assert available
	clusterNodes, err = r.buildClusterNodes(ctx, valkeyCluster)
	if err != nil {
		log.Error(err, "Failed to build cluster nodes")
		return ctrl.Result{}, err
	}

	clusterNodesMap := make(map[*internalValkey.ClusterNode][]*internalValkey.ClusterNode)
	for _, cn := range clusterNodes {
		if cn.IsMaster() {
			clusterNodesMap[cn] = make([]*internalValkey.ClusterNode, 0)
		}
	}
	for _, cn := range clusterNodes {
		if !cn.IsMaster() {
			for masterNode, _ := range clusterNodesMap {
				if masterNode.ID == cn.MasterNodeID {
					clusterNodesMap[masterNode] = append(clusterNodesMap[masterNode], cn)
				}
			}
		}
	}
	isAvailable := true
	if len(clusterNodesMap) != int(valkeyCluster.Spec.Shards) {
		isAvailable = false
	}
	for _, v := range clusterNodesMap {
		if len(v) != int(valkeyCluster.Spec.Replicas) {
			isAvailable = false
		}
	}
	expectedSlotCounts := internalValkey.SlotCounts(int(valkeyCluster.Spec.Shards))
	sort.Ints(expectedSlotCounts)
	actualSlotCounts := make([]int, 0)
	for _, cn := range clusterNodes {
		if cn.IsMaster() {
			actualSlotCounts = append(actualSlotCounts, cn.SlotCount())
		}
	}
	sort.Ints(actualSlotCounts)

	if len(expectedSlotCounts) != len(actualSlotCounts) {
		isAvailable = false
	} else {
		for idx, actual := range actualSlotCounts {
			if actual != expectedSlotCounts[idx] {
				isAvailable = false
			}
		}
	}

	if isAvailable {
		if err := r.Get(ctx, req.NamespacedName, valkeyCluster); err != nil {
			log.Error(err, "Failed to re-fetch valkeyCluster")
			return ctrl.Result{}, err
		}

		var currentConditionStatus metav1.ConditionStatus
		for _, condition := range valkeyCluster.Status.Conditions {
			if condition.Type == typeAvailableValkeyCluster {
				log.Info("found available valkey cluster condition", "condition", condition, "type", condition.Type, "status", condition.Status)
				currentConditionStatus = condition.Status
			}
		}

		if currentConditionStatus != metav1.ConditionTrue {
			meta.SetStatusCondition(&valkeyCluster.Status.Conditions, metav1.Condition{Type: typeAvailableValkeyCluster,
				Status: metav1.ConditionTrue, Reason: "Reconciling",
				Message: fmt.Sprintf("Cluster for custom resource (%s) is avaiable", valkeyCluster.Name)})
			if err := r.Status().Update(ctx, valkeyCluster); err != nil {
				log.Error(err, "Failed to update ValkeyCluster status")
				return ctrl.Result{}, err
			}
		}
	}

	return ctrl.Result{}, nil
}

func (r *ValkeyClusterReconciler) waitForPodsToBeAccessibleViaValkey(ctx context.Context, valkeyCluster *cachev1alpha1.ValkeyCluster) (*ctrl.Result, error) {
	logger := log.FromContext(ctx)
	podList := &corev1.PodList{}
	listOpts := []client.ListOption{
		client.InNamespace(valkeyCluster.Namespace),
		client.MatchingLabels(labelsForValkeyCluster(valkeyCluster.Name)),
	}
	if err := r.List(ctx, podList, listOpts...); err != nil {
		logger.Error(err, "Failed to list pods", "ValkeyCluster.Namespace", valkeyCluster.Namespace, "ValkeyCluster.Name", valkeyCluster.Name)
		return nil, err
	}
	for _, pod := range podList.Items {
		if pod.Status.Phase != corev1.PodRunning {
			logger.Info("Pod not running", "Pod.Name", pod.Name, "Pod.Status", pod.Status.Phase)
			return &ctrl.Result{RequeueAfter: time.Minute}, nil
		}

		logger.Info("Pod running", "Pod.Name", pod.Name, "Pod.Status", pod.Status.Phase)

		isPodReady := false
		for _, condition := range pod.Status.Conditions {
			if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
				isPodReady = true
			}
		}
		if isPodReady {
			client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{pod.Status.PodIP + ":6379"}, ForceSingleClient: true})
			if err != nil {
				logger.Info("Could not create valkey client, requeing")
				return &ctrl.Result{RequeueAfter: time.Minute}, nil
			}
			defer client.Close()
			_, err = client.Do(ctx, client.B().ClusterNodes().Build()).ToString()
			if err != nil {
				logger.Info("Could not get cluster nodes requeing")
				return &ctrl.Result{RequeueAfter: time.Minute}, nil
			}
		} else {
			logger.Info("Pod not ready", "Pod.Name", pod.Name)
			return &ctrl.Result{RequeueAfter: time.Minute}, nil
		}
	}
	return nil, nil
}

// This method can request a requeue. A nil value is considered nothing, if it's not nil the value is used to requeue
func (r *ValkeyClusterReconciler) reconcileValkeySlots(ctx context.Context, valkeyCluster *cachev1alpha1.ValkeyCluster) (*ctrl.Result, error) {
	logger := log.FromContext(ctx)
	var result *ctrl.Result

	clusterNodesForShard, err := r.buildClusterNodesForShard(ctx, valkeyCluster)
	if err != nil {
		return result, fmt.Errorf("Failed to build cluster nodes for shard: %v", err)
	}

	if len(clusterNodesForShard) != int(valkeyCluster.Spec.Shards) {
		// scaling action should trigger requeue
		result = &ctrl.Result{Requeue: true}
	}

	actionPlan, err := internalValkey.GenerateReshardingPlan(clusterNodesForShard, int(valkeyCluster.Spec.Shards))
	if err != nil {
		return result, fmt.Errorf("Failed to build action plan: %v", err)
	}

	for _, plan := range actionPlan {
		logger.Info(fmt.Sprintf("%s/%s: Begin migrating %d slots from %s to %s", valkeyCluster.Namespace, valkeyCluster.Name, plan.Slots, plan.FromID, plan.ToID))
		_, _, err := r.executeValkeyCli(ctx, valkeyCluster, []string{"--cluster", "reshard", "127.0.0.1:6379",
			"--cluster-from", plan.FromID,
			"--cluster-to", plan.ToID,
			"--cluster-slots", fmt.Sprintf("%d", plan.Slots),
			"--cluster-yes"})
		if err != nil {
			return nil, err
		}
		r.Recorder.Event(valkeyCluster, "Normal", "Migrated slots",
			fmt.Sprintf("Migrated %d slots from %s to %s", plan.Slots, plan.FromID, plan.ToID))
		logger.Info(fmt.Sprintf("%s/%s: Successfully migrated %d slots from %s to %s", valkeyCluster.Namespace, valkeyCluster.Name, plan.Slots, plan.FromID, plan.ToID))

	}
	return result, nil
}

func (r *ValkeyClusterReconciler) updateClusterNodesStatus(ctx context.Context, req ctrl.Request) error {
	logger := log.FromContext(ctx)
	valkeyCluster := &cachev1alpha1.ValkeyCluster{}
	if err := r.Get(ctx, req.NamespacedName, valkeyCluster); err != nil {
		return err
	}
	clusterNodes, err := r.buildClusterNodes(ctx, valkeyCluster)
	if err != nil {
		return err
	}

	clusterNodesStatus := make(map[string][]cachev1alpha1.ValkeyClusterNode, 0)
	for _, clusterNode := range clusterNodes {
		re := regexp.MustCompile(valkeyCluster.Name + `-([\d]+)-([\d]+)`)
		matches := re.FindAllStringSubmatch(clusterNode.Pod, -1)
		shardIdx := matches[0][1]
		if _, ok := clusterNodesStatus["shard:"+shardIdx]; !ok {
			clusterNodesStatus["shard:"+shardIdx] = make([]cachev1alpha1.ValkeyClusterNode, 0)
		}
		clusterNodesStatus["shard:"+shardIdx] = append(clusterNodesStatus["shard:"+shardIdx], internalValkey.ToStatusClusterNode(*clusterNode))
	}

	for k := range clusterNodesStatus {
		sort.Slice(clusterNodesStatus[k], func(i, j int) bool {
			return clusterNodesStatus[k][i].Pod < clusterNodesStatus[k][j].Pod
		})
	}

	needsUpdate := false
	if len(valkeyCluster.Status.ClusterNodes) != len(clusterNodesStatus) {
		needsUpdate = true
	}
	if !reflect.DeepEqual(valkeyCluster.Status.ClusterNodes, clusterNodesStatus) {
		needsUpdate = true
	}

	if needsUpdate {
		valkeyCluster.Status.ClusterNodes = make(map[string][]cachev1alpha1.ValkeyClusterNode)
		for k, v := range clusterNodesStatus {
			for _, n := range v {
				valkeyCluster.Status.ClusterNodes[k] = append(valkeyCluster.Status.ClusterNodes[k], n)
			}
		}
		valkeyCluster.Status.ClusterNodes = clusterNodesStatus
		if err := r.Status().Update(ctx, valkeyCluster); err != nil {
			return err
		}
		logger.Info(fmt.Sprintf("Valkey cluster %s/%s status updated", valkeyCluster.Namespace, valkeyCluster.Name))
	}
	return nil
}

func (r *ValkeyClusterReconciler) buildClusterNodesForShard(ctx context.Context, valkeyCluster *cachev1alpha1.ValkeyCluster) (map[int][]*internalValkey.ClusterNode, error) {
	clusterNodes, err := r.buildClusterNodes(ctx, valkeyCluster)
	if err != nil {
		return nil, err
	}
	re := regexp.MustCompile(fmt.Sprintf(`%s-(\d+)-(\d+)`, valkeyCluster.Name))
	clusterNodesForShard := make(map[int][]*internalValkey.ClusterNode)
	for _, cn := range clusterNodes {
		matches := re.FindAllStringSubmatch(cn.Pod, -1)
		shardIdx, err := strconv.Atoi(matches[0][1])
		if err != nil {
			return nil, fmt.Errorf("could not parse shard index: %v", err)
		}
		if _, ok := clusterNodesForShard[shardIdx]; !ok {
			clusterNodesForShard[shardIdx] = make([]*internalValkey.ClusterNode, 0)
		}
		clusterNodesForShard[shardIdx] = append(clusterNodesForShard[shardIdx], cn)
	}
	return clusterNodesForShard, nil
}

func (r *ValkeyClusterReconciler) buildClusterNodes(ctx context.Context, valkeyCluster *cachev1alpha1.ValkeyCluster) ([]*internalValkey.ClusterNode, error) {
	clusterNodes := []*internalValkey.ClusterNode{}
	podList := &corev1.PodList{}
	listOpts := []client.ListOption{
		client.InNamespace(valkeyCluster.Namespace),
		client.MatchingLabels(labelsForValkeyCluster(valkeyCluster.Name)),
	}
	if err := r.List(ctx, podList, listOpts...); err != nil {
		return nil, err
	}
	for _, pod := range podList.Items {
		if pod.Status.Phase != corev1.PodRunning {
			continue
		}
		isPodReady := false
		for _, condition := range pod.Status.Conditions {
			if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
				isPodReady = true
			}
		}
		if isPodReady {
			client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{pod.Status.PodIP + ":6379"}, ForceSingleClient: true})
			if err != nil {
				return nil, err
			}
			defer client.Close()
			clusterNodesTxt, err := client.Do(ctx, client.B().ClusterNodes().Build()).ToString()
			if err != nil {
				return nil, err
			}
			cn, err := internalValkey.ParseClusterNode(clusterNodesTxt)
			if err != nil {
				return nil, err
			}
			cn.Pod = pod.Name
			clusterNodes = append(clusterNodes, cn)
		} else {
			continue
		}
	}
	return clusterNodes, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ValkeyClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	var err error
	r.ClientSet, err = kubernetes.NewForConfig(mgr.GetConfig())
	if err != nil {
		return err
	}
	r.RestConfig = mgr.GetConfig()
	return ctrl.NewControllerManagedBy(mgr).
		For(&cachev1alpha1.ValkeyCluster{}).
		Owns(&appsv1.StatefulSet{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: 2}).
		Complete(r)
}

// finalizeValkeyCluster will perform the required operations before delete the CR.
func (r *ValkeyClusterReconciler) doFinalizerOperationsForValkeyCluster(cr *cachev1alpha1.ValkeyCluster) {
	// TODO(user): Add the cleanup steps that the operator
	// needs to do before the CR can be deleted. Examples
	// of finalizers include performing backups and deleting
	// resources that are not owned by this CR, like a PVC.

	// Note: It is not recommended to use finalizers with the purpose of deleting resources which are
	// created and managed in the reconciliation. These ones, such as the StatefulSet created on this reconcile,
	// are defined as dependent of the custom resource. See that we use the method ctrl.SetControllerReference.
	// to set the ownerRef which means that the StatefulSet will be deleted by the Kubernetes API.
	// More info: https://kubernetes.io/docs/tasks/administer-cluster/use-cascading-deletion/

	// The following implementation will raise an event
	r.Recorder.Event(cr, "Warning", "Deleting",
		fmt.Sprintf("Custom Resource %s is being deleted from the namespace %s",
			cr.Name,
			cr.Namespace))
}

// persistentVolumeClaim returns a ValkeyCluster PVC object
func (r *ValkeyClusterReconciler) persistentVolumeClaim(name string, valkeyCluster *cachev1alpha1.ValkeyCluster) (*corev1.PersistentVolumeClaim, error) {
	ls := labelsForValkeyCluster(valkeyCluster.Name)
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: valkeyCluster.Namespace,
			Labels:    ls,
		},
		Spec: *valkeyCluster.Spec.Storage,
	}
	// Set the ownerRef for the PersistentVolumeClaim
	// More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/owners-dependents/
	if err := ctrl.SetControllerReference(valkeyCluster, pvc, r.Scheme); err != nil {
		return nil, err
	}

	return pvc, nil
}

func (r *ValkeyClusterReconciler) upsertHeadlessService(ctx context.Context, valkeyCluster *cachev1alpha1.ValkeyCluster) error {
	logger := log.FromContext(ctx)
	logger.Info("upserting headless service")

	name := valkeyCluster.Name + "-headless"
	ls := labelsForValkeyCluster(valkeyCluster.Name)
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: valkeyCluster.Namespace,
			Labels:    ls,
		},
		Spec: corev1.ServiceSpec{
			Selector:  ls,
			ClusterIP: "None",
			Ports: []corev1.ServicePort{
				{
					Port:       6379,
					TargetPort: intstr.FromInt(6379),
				},
			},
		},
	}
	if err := controllerutil.SetControllerReference(valkeyCluster, svc, r.Scheme); err != nil {
		return err
	}
	if err := r.Create(ctx, svc); err != nil {
		if errors.IsAlreadyExists(err) {
			found := &corev1.Service{}
			if err = r.Get(ctx, types.NamespacedName{Name: name, Namespace: valkeyCluster.Namespace}, found); err != nil {
				logger.Error(err, "failed to get Service")
			}
			needsUpdate := false

			if len(found.Spec.Ports) != len(svc.Spec.Ports) {
				needsUpdate = true
			} else {
				for i, port := range found.Spec.Ports {
					if port.Port != svc.Spec.Ports[i].Port {
						needsUpdate = true
					}
					if port.TargetPort != svc.Spec.Ports[i].TargetPort {
						needsUpdate = true
					}
				}
			}

			found.Spec = svc.Spec
			if needsUpdate {
				if err := r.Update(ctx, found); err != nil {
					logger.Error(err, "failed to update Service")
					return err
				}
				logger.Info("Service updated")
			}
		} else {
			logger.Error(err, "failed to create Service")
			return err
		}
	} else {
		r.Recorder.Event(valkeyCluster, "Normal", "Created",
			fmt.Sprintf("Service %s/%s is created", valkeyCluster.Namespace, name))
	}
	return nil

}

// statefulSet returns a ValkeyCluster StatefulSet object
func (r *ValkeyClusterReconciler) statefulSet(name string, size int32, valkeyCluster *cachev1alpha1.ValkeyCluster) *appsv1.StatefulSet {
	ls := labelsForValkeyCluster(valkeyCluster.Name)
	ls["statefulset.kubernetes.io/sts-name"] = name

	affinityTerms := []corev1.WeightedPodAffinityTerm{}
	for _, topologyKey := range valkeyCluster.Spec.AntiAffinityTopologyKeys {
		affinityTerms = append(affinityTerms, corev1.WeightedPodAffinityTerm{
			Weight: 1,
			PodAffinityTerm: corev1.PodAffinityTerm{
				LabelSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						"statefulset.kubernetes.io/sts-name": name,
						"cache/name":                         valkeyCluster.Name,
					},
				},
				TopologyKey: topologyKey,
			},
		})
	}
	var affinity *corev1.Affinity
	if len(affinityTerms) > 0 {
		affinity = &corev1.Affinity{
			PodAntiAffinity: &corev1.PodAntiAffinity{
				PreferredDuringSchedulingIgnoredDuringExecution: affinityTerms,
			},
		}
	}

	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: valkeyCluster.Namespace,
			Labels:    ls,
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: valkeyCluster.Name + "-headless",
			Replicas:    &size,
			Selector: &metav1.LabelSelector{
				MatchLabels: ls,
			},
			MinReadySeconds: valkeyCluster.Spec.MinReadySeconds,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: ls,
					Annotations: map[string]string{
						"prometheus.io/port": "9121",
						"prometheus.io/path": "/metrics",
					},
				},
				Spec: corev1.PodSpec{
					NodeSelector: valkeyCluster.Spec.NodeSelector,
					Tolerations:  valkeyCluster.Spec.Tolerations,
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot: &[]bool{true}[0],
						// IMPORTANT: seccomProfile was introduced with Kubernetes 1.19
						// If you are looking for to produce solutions to be supported
						// on lower versions you must remove this option.
						SeccompProfile: &corev1.SeccompProfile{
							Type: corev1.SeccompProfileTypeRuntimeDefault,
						},
						FSGroup: &[]int64{1009}[0],
					},
					Containers: []corev1.Container{
						{
							Image:           valkeyCluster.Spec.Image,
							Name:            "valkey-cluster-node",
							ImagePullPolicy: corev1.PullIfNotPresent,
							Resources:       *valkeyCluster.Spec.Resources,

							// Ensure restrictive context for the container
							// More info: https://kubernetes.io/docs/concepts/security/pod-security-standards/#restricted
							SecurityContext: &corev1.SecurityContext{
								// WARNING: Ensure that the image used defines an UserID in the Dockerfile
								// otherwise the Pod will not run and will fail with "container has runAsNonRoot and image has non-numeric user"".
								// If you want your workloads admitted in namespaces enforced with the restricted mode in OpenShift/OKD vendors
								// then, you MUST ensure that the Dockerfile defines a User ID OR you MUST leave the "RunAsNonRoot" and
								// "RunAsUser" fields empty.
								RunAsNonRoot: &[]bool{true}[0],
								// The valkeyCluster image does not use a non-zero numeric user as the default user.
								// Due to RunAsNonRoot field being set to true, we need to force the user in the
								// container to a non-zero numeric user. We do this using the RunAsUser field.
								// However, if you are looking to provide solution for K8s vendors like OpenShift
								// be aware that you cannot run under its restricted-v2 SCC if you set this value.
								RunAsUser:                &[]int64{1001}[0],
								AllowPrivilegeEscalation: &[]bool{false}[0],
								Capabilities: &corev1.Capabilities{
									Drop: []corev1.Capability{
										"ALL",
									},
								},
							},
							Ports: []corev1.ContainerPort{
								{
									ContainerPort: 6379,
									Name:          "valkey-tcp",
								},
								{
									ContainerPort: 16379,
									Name:          "valkey-bus",
								},
							},
							Lifecycle: &corev1.Lifecycle{
								PreStop: &corev1.LifecycleHandler{
									Exec: &corev1.ExecAction{
										Command: []string{"/bin/sh", "/scripts/pre_stop.sh"},
									},
								},
								PostStart: &corev1.LifecycleHandler{
									Exec: &corev1.ExecAction{
										Command: []string{"/bin/sh", "/scripts/post_start.sh"},
									},
								},
							},
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									TCPSocket: &corev1.TCPSocketAction{
										Port: intstr.FromInt(6379),
									},
								},
							},
							LivenessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									TCPSocket: &corev1.TCPSocketAction{
										Port: intstr.FromInt(6379),
									},
								},
							},
							Env: []corev1.EnvVar{
								{
									Name: "POD_IP",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{
											APIVersion: "v1",
											FieldPath:  "status.podIP",
										},
									},
								},
								{
									Name:  "NODE_HOSTNAME_SUFFIX",
									Value: "." + valkeyCluster.Name + "-headless." + valkeyCluster.Namespace + ".svc.cluster.local",
								},
							},
							WorkingDir: "/data",
							Command:    []string{"sh", "-c", `exec valkey-server ./valkey.conf --cluster-announce-client-ipv4 $POD_IP --cluster-announce-hostname "${HOSTNAME}${NODE_HOSTNAME_SUFFIX}" --cluster-announce-human-nodename $HOSTNAME`},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "valkey-data",
									MountPath: "/data",
								},
								{
									Name:      "valkey-configmap",
									MountPath: "/data/valkey.conf",
									SubPath:   "valkey.conf",
									ReadOnly:  true,
								},
								{
									Name:      "valkey-configmap",
									MountPath: "/scripts",
								},
							},
						},
						{
							Image:           "quay.io/oliver006/redis_exporter:v1.73.0-alpine",
							Name:            "valkey-exporter",
							ImagePullPolicy: corev1.PullIfNotPresent,
							Resources: *&corev1.ResourceRequirements{
								Limits: map[corev1.ResourceName]resource.Quantity{
									corev1.ResourceCPU:    resource.MustParse("100m"),
									corev1.ResourceMemory: resource.MustParse("64Mi"),
								},
								Requests: map[corev1.ResourceName]resource.Quantity{
									corev1.ResourceCPU:    resource.MustParse("100m"),
									corev1.ResourceMemory: resource.MustParse("64Mi"),
								},
							},
							Ports: []corev1.ContainerPort{
								{
									ContainerPort: 9121,
									Name:          "metrivs",
								},
							},
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									TCPSocket: &corev1.TCPSocketAction{
										Port: intstr.FromInt(9121),
									},
								},
							},
							LivenessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									TCPSocket: &corev1.TCPSocketAction{
										Port: intstr.FromInt(9121),
									},
								},
							},
							Args: []string{"-is-cluster"},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "valkey-configmap",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: valkeyCluster.Name,
									},
								},
							},
						},
					},
					Affinity: affinity,
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "valkey-data",
					Labels: ls,
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes:      valkeyCluster.Spec.Storage.AccessModes,
					StorageClassName: valkeyCluster.Spec.Storage.StorageClassName,
					Resources:        valkeyCluster.Spec.Storage.Resources,
				},
			}},
		},
	}

	return sts
}

// labelsForValkeyCluster returns the labels for selecting the resources
// More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/common-labels/
func labelsForValkeyCluster(name string) map[string]string {
	var imageTag string
	image, err := imageForValkeyCluster()
	if err == nil {
		imageTag = strings.Split(image, ":")[1]
	}
	return map[string]string{
		"cache/name":                   name,
		"app.kubernetes.io/name":       "valkeyCluster-operator",
		"app.kubernetes.io/version":    imageTag,
		"app.kubernetes.io/managed-by": "ValkeyClusterController",
	}
}

// imageForValkeyCluster gets the Operand image which is managed by this controller
// from the VALKEYCLUSTER_IMAGE environment variable defined in the config/manager/manager.yaml
func imageForValkeyCluster() (string, error) {
	var imageEnvVar = "VALKEYCLUSTER_IMAGE"
	image, found := os.LookupEnv(imageEnvVar)
	if !found {
		return "ghcr.io/halter/valkey-server:8.0.2", nil
	}
	return image, nil
}

func (r *ValkeyClusterReconciler) upsertConfigMap(ctx context.Context, valkeyCluster *cachev1alpha1.ValkeyCluster) error {
	logger := log.FromContext(ctx)
	logger.Info("upserting configmap")

	preStop, err := scripts.ReadFile("scripts/pre_stop.sh")
	if err != nil {
		logger.Error(err, "failed to read pre_stop.sh")
		return err
	}
	postStart, err := scripts.ReadFile("scripts/post_start.sh")
	if err != nil {
		logger.Error(err, "failed to read post_start.sh")
		return err
	}
	valkeyConf, err := scripts.ReadFile("scripts/valkey.conf")
	if err != nil {
		logger.Error(err, "failed to read valkey.conf")
		return err
	}
	ls := labelsForValkeyCluster(valkeyCluster.Name)
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      valkeyCluster.Name,
			Namespace: valkeyCluster.Namespace,
			Labels:    ls,
		},
		Data: map[string]string{
			"pre_stop.sh":   string(preStop),
			"post_start.sh": string(postStart),
			"valkey.conf":   string(valkeyConf),
		},
	}
	if err := controllerutil.SetControllerReference(valkeyCluster, cm, r.Scheme); err != nil {
		return err
	}
	if err := r.Create(ctx, cm); err != nil {
		if errors.IsAlreadyExists(err) {
			found := &corev1.ConfigMap{}
			if err = r.Get(ctx, types.NamespacedName{Name: valkeyCluster.Name, Namespace: valkeyCluster.Namespace}, found); err != nil {
				logger.Error(err, "failed to get ConfigMap")
			}
			needsUpdate := false
			if len(found.Data) != len(cm.Data) {
				needsUpdate = true
			}
			for k, v := range found.Data {
				if v != cm.Data[k] {
					needsUpdate = true
				}
			}

			if needsUpdate {
				if err := r.Update(ctx, cm); err != nil {
					logger.Error(err, "failed to update ConfigMap")
					return err
				}
				logger.Info("configmap updated")
			}
		} else {
			logger.Error(err, "failed to create ConfigMap")
			return err
		}
	} else {
		r.Recorder.Event(valkeyCluster, "Normal", "Created",
			fmt.Sprintf("ConfigMap %s/%s is created", valkeyCluster.Namespace, valkeyCluster.Name))
	}
	return nil
}

func statefulSetSize(valkeyCluster *cachev1alpha1.ValkeyCluster) int32 {
	return 1 + valkeyCluster.Spec.Replicas
}

func (r *ValkeyClusterReconciler) executeValkeyCli(ctx context.Context, valkeyCluster *cachev1alpha1.ValkeyCluster, args []string) (string, string, error) {
	cmd := []string{
		"sh",
		"-c",
		fmt.Sprintf("valkey-cli %s", strings.Join(args, " ")),
	}

	podName := fmt.Sprintf("%s-0-0", valkeyCluster.Name)

	req := r.ClientSet.CoreV1().RESTClient().Post().Resource("pods").Name(podName).
		Namespace(valkeyCluster.Namespace).SubResource("exec")
	req.VersionedParams(&corev1.PodExecOptions{
		Container: "valkey-cluster-node",
		Command:   cmd,
		Stdin:     false,
		Stdout:    true,
		Stderr:    true,
		TTY:       false,
	}, runtime.NewParameterCodec(r.Scheme))
	exec, err := remotecommand.NewSPDYExecutor(r.RestConfig, "POST", req.URL())
	if err != nil {
		return "", "", fmt.Errorf("Failed to execute valkey-cli %s: %v", strings.Join(args, " "), err)
	}
	var stdout, stderr bytes.Buffer
	err = exec.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: &stdout,
		Stderr: &stderr,
	})
	if err != nil {
		return "", "", fmt.Errorf("Failed executing command 'valkey-cli %s': stdout: %s, stderr: %s, err: %v", strings.Join(args, " "), stdout.String(), stderr.String(), err)
	}
	return stdout.String(), stderr.String(), nil
}
