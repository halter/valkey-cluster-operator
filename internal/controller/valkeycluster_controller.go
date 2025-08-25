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

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cachev1alpha1 "github.com/halter/valkey-cluster-operator/api/v1alpha1"
	"github.com/halter/valkey-cluster-operator/internal/controller/valkey"
	internalValkey "github.com/halter/valkey-cluster-operator/internal/controller/valkey"
)

//go:embed scripts/*
var scripts embed.FS

const valkeyClusterFinalizer = "cache.halter.io/finalizer"
const valkeyConfigAnnotation = "cache.halter.io/config-hash"

const VALKEY_PORT int = 6379
const VALKEY_BUS_PORT int = 16379

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

	// We may add the option to force a restart in future using the hash of the updated valkey config
	_, err = r.upsertConfigMap(ctx, valkeyCluster)
	if err != nil {
		log.Error(err, "Failed to upsert configmap")
		return ctrl.Result{}, err
	}

	err = r.upsertHeadlessService(ctx, valkeyCluster)
	if err != nil {
		log.Error(err, "Failed to upsert Service")
		return ctrl.Result{}, err
	}

	res, err := r.reconcileStatefulSets(ctx, req, valkeyCluster)
	if err != nil {
		log.Error(err, "Failed to reconcile statefulsets")
		return *res, err
	}

	// wait for all pods to be running and accessible via valkey-client
	res, err = r.waitForPodsToBeAccessibleViaValkey(ctx, valkeyCluster)
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
				_, _, err := r.executeValkeyCli(ctx, valkeyCluster, []string{"--cluster", "del-node", fmt.Sprintf("127.0.0.1:%d", VALKEY_PORT), cn.ID})
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
				client, err := r.NewValkeyClient(ctx, valkeyCluster, clusterNodeA.IP, VALKEY_PORT)
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

				err = client.Do(ctx, client.B().ClusterMeet().Ip(clusterNodeB.IP).Port(int64(VALKEY_PORT)).Build()).Error()
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
			client, err := r.NewValkeyClient(ctx, valkeyCluster, clusterNodesForShard[0].IP, VALKEY_PORT)
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
					client, err := r.NewValkeyClient(ctx, valkeyCluster, cn.IP, VALKEY_PORT)
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
				client, err := r.NewValkeyClient(ctx, valkeyCluster, cn.IP, VALKEY_PORT)
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
			// check tcp port
			if !valkey.TcpCheck(pod.Status.PodIP, fmt.Sprintf("%d", VALKEY_PORT)) {
				return &ctrl.Result{RequeueAfter: 15 * time.Second}, nil
			}

			client, err := r.NewValkeyClient(ctx, valkeyCluster, pod.Status.PodIP, VALKEY_PORT)
			if err != nil {
				logger.Info("Could not create valkey client, requeing")
				return &ctrl.Result{RequeueAfter: 15 * time.Second}, nil
			}
			defer client.Close()
			_, err = client.Do(ctx, client.B().ClusterNodes().Build()).ToString()
			if err != nil {
				logger.Info("Could not get cluster nodes requeing")
				return &ctrl.Result{RequeueAfter: 15 * time.Second}, nil
			}
		} else {
			logger.Info("Pod not ready", "Pod.Name", pod.Name)
			return &ctrl.Result{RequeueAfter: 15 * time.Second}, nil
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
			client, err := r.NewValkeyClient(ctx, valkeyCluster, pod.Status.PodIP, VALKEY_PORT)
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
