package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/google/go-cmp/cmp"
	cachev1alpha1 "github.com/halter/valkey-cluster-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

func (r *ValkeyClusterReconciler) findStatefulsetByName(ctx context.Context, namespace, name string) (*appsv1.StatefulSet, error) {
	found := &appsv1.StatefulSet{}
	err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, found)
	return found, err
}

func (r *ValkeyClusterReconciler) updateStatefulSet(ctx context.Context, namespace, name string, updateFunc func(*appsv1.StatefulSet) *appsv1.StatefulSet) error {
	logger := log.FromContext(ctx)
	found, err := r.findStatefulsetByName(ctx, namespace, name)
	if err != nil {
		logger.Error(err, fmt.Sprintf("Could not find StatefulSet %s/%s", namespace, name))
		return err
	}
	updated := updateFunc(found)
	err = r.Update(ctx, updated)
	if err != nil {
		logger.Error(err, fmt.Sprintf("Failed to update StatefulSet %s/%s", namespace, name))
		return err
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

	metricsContainerArgs := []string{"-is-cluster"}
	if valkeyCluster.Spec.Password != "" {
		metricsContainerArgs = append(metricsContainerArgs, "--redis.password", valkeyCluster.Spec.Password)
	}

	var command []string
	// this is to preserve the behaviour of clusters running the old valkey image. the new images are under the namespace: ghcr.io/halter/valkey-server and have an entrypoint that handles running valkey-server
	if valkeyCluster.Spec.Image == "ghcr.io/halter/valkey:8.0.2" {
		command = []string{"sh", "-c", `exec valkey-server ./valkey.conf --cluster-announce-client-ipv4 $POD_IP --cluster-announce-human-nodename "${HOSTNAME}${NODE_HOSTNAME_SUFFIX}"`}
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
			MinReadySeconds:     valkeyCluster.Spec.MinReadySeconds,
			PodManagementPolicy: appsv1.ParallelPodManagement, // TODO: reconcile statefulsets that do not have this option set
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
									ContainerPort: int32(VALKEY_PORT),
									Name:          "valkey-tcp",
								},
								{
									ContainerPort: int32(VALKEY_BUS_PORT),
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
									Exec: &corev1.ExecAction{
										Command: []string{"/bin/bash", "/scripts/readiness.sh"},
									},
								},
								InitialDelaySeconds: valkeyCluster.Spec.InitialDelaySeconds,
								TimeoutSeconds:      5,
								PeriodSeconds:       10,
								SuccessThreshold:    1,
								FailureThreshold:    100,
							},
							LivenessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									TCPSocket: &corev1.TCPSocketAction{
										Port: intstr.FromInt(VALKEY_PORT),
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
							Command:    command,
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
							Image:           "quay.io/oliver006/redis_exporter:v1.75.0-alpine",
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
									Name:          "metrics",
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
							Args: metricsContainerArgs,
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

func statefulSetSize(valkeyCluster *cachev1alpha1.ValkeyCluster) int32 {
	return 1 + valkeyCluster.Spec.Replicas
}

func (r *ValkeyClusterReconciler) reconcileStatefulSets(ctx context.Context, req ctrl.Request, valkeyCluster *cachev1alpha1.ValkeyCluster) (*ctrl.Result, error) {
	log := log.FromContext(ctx)
	// Check if the statefulset already exists, if not create a new one
	for stsIdx := 0; stsIdx < int(valkeyCluster.Spec.Shards); stsIdx++ {
		stsName := fmt.Sprintf("%s-%d", valkeyCluster.Name, stsIdx)
		found, err := r.findStatefulsetByName(ctx, valkeyCluster.Namespace, stsName)
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
					return &ctrl.Result{}, err
				}

				return &ctrl.Result{}, err
			}

			log.Info("Creating a new StatefulSet",
				"StatefulSet.Namespace", sts.Namespace, "StatefulSet.Name", sts.Name)
			if err = r.Create(ctx, sts); err != nil {
				log.Error(err, "Failed to create new StatefulSet",
					"StatefulSet.Namespace", sts.Namespace, "StatefulSet.Name", sts.Name)
				return &ctrl.Result{}, err
			}
			r.Recorder.Event(valkeyCluster, "Normal", "Created",
				fmt.Sprintf("StatefulSet %s/%s is created", valkeyCluster.Namespace, sts.Name))

			// StatefulSet created successfully
			// We will requeue the reconciliation so that we can ensure the state
			// and move forward for the next operations
			return &ctrl.Result{RequeueAfter: 15 * time.Second}, nil
		} else if err != nil {
			log.Error(err, "Failed to get StatefulSet")
			// Let's return the error for the reconciliation be re-trigged again
			return &ctrl.Result{}, err
		}

		// check if any update is occuring for stateful set, if so re-schedule reconcile
		found, err = r.findStatefulsetByName(ctx, valkeyCluster.Namespace, stsName)
		if err != nil {
			log.Error(err, fmt.Sprintf("Failed to get StatefulSet %s/%s", valkeyCluster.Namespace, stsName))
			return &ctrl.Result{}, err
		}
		if found.Status.CurrentRevision != found.Status.UpdateRevision {
			return &ctrl.Result{RequeueAfter: 15 * time.Second}, nil
		}

		// update containers[0].Command if there is a difference
		// Update if there is a difference in the following attributes:
		// Command
		// Lifecycle
		// Env
		diff, err := r.compareActualToDesiredStatefulSet(ctx, valkeyCluster, stsName)
		if err != nil {
			log.Error(err, fmt.Sprintf("Failed to compare actual and desired StatefulSet %s/%s", valkeyCluster.Namespace, stsName))
			return &ctrl.Result{}, err
		}
		if diff {
			err := r.updateStatefulSet(ctx, valkeyCluster.Namespace, stsName, r.applyDesiredStatefulSetSpec(valkeyCluster, stsName))

			if err != nil {
				log.Error(err, "Failed to update StatefulSet",
					"StatefulSet.Namespace", found.Namespace, "StatefulSet.Name", found.Name)
				return &ctrl.Result{}, err
			}

			r.Recorder.Event(valkeyCluster, "Normal", "Updated",
				fmt.Sprintf("StatefulSet Containers are updated for %s/%s", found.Namespace, found.Name))

			return &ctrl.Result{RequeueAfter: 15 * time.Second}, nil
		}

		// We can simply change the number of replicas inside the shard as that has no impact on data availabilty
		if *found.Spec.Replicas != (statefulSetSize(valkeyCluster)) && *found.Spec.Replicas < (statefulSetSize(valkeyCluster)) {
			log.Info(fmt.Sprintf("StatefulSet needs to increase replicas from %d to %d", *found.Spec.Replicas, (valkeyCluster.Spec.Shards + valkeyCluster.Spec.Replicas)))
			err := r.updateStatefulSet(ctx, valkeyCluster.Namespace, stsName, func(ss *appsv1.StatefulSet) *appsv1.StatefulSet {
				ss.Spec.Replicas = &[]int32{(statefulSetSize(valkeyCluster))}[0]
				return ss
			})
			if err != nil {
				log.Error(err, "Failed to update StatefulSet",
					"StatefulSet.Namespace", found.Namespace, "StatefulSet.Name", found.Name)

				// Re-fetch the valkeyCluster Custom Resource before updating the status
				// so that we have the latest state of the resource on the cluster and we will avoid
				// raising the error "the object has been modified, please apply
				// your changes to the latest version and try again" which would re-trigger the reconciliation
				if err := r.Get(ctx, req.NamespacedName, valkeyCluster); err != nil {
					log.Error(err, "Failed to re-fetch valkeyCluster")
					return &ctrl.Result{}, err
				}

				// The following implementation will update the status
				meta.SetStatusCondition(&valkeyCluster.Status.Conditions, metav1.Condition{Type: typeAvailableValkeyCluster,
					Status: metav1.ConditionFalse, Reason: "Resizing",
					Message: fmt.Sprintf("Failed to update the size for the custom resource (%s): (%s)", valkeyCluster.Name, err)})

				if err := r.Status().Update(ctx, valkeyCluster); err != nil {
					log.Error(err, "Failed to update ValkeyCluster status")
					return &ctrl.Result{}, err
				}

				return &ctrl.Result{}, err
			}

			log.Info("StatefulSet replicas updated")

			// Now, that we update the size we want to requeue the reconciliation
			// so that we can ensure that we have the latest state of the resource before
			// update. Also, it will help ensure the desired state on the cluster
			return &ctrl.Result{Requeue: true}, nil
		}

		foundResources := found.Spec.Template.Spec.Containers[0].Resources
		log.Info(fmt.Sprintf("StatefulSet resources: %v", foundResources))

		foundRequests := foundResources.Requests
		foundLimits := foundResources.Limits

		if foundRequests == nil || foundLimits == nil {
			err := r.updateStatefulSet(ctx, valkeyCluster.Namespace, stsName, func(ss *appsv1.StatefulSet) *appsv1.StatefulSet {
				ss.Spec.Template.Spec.Containers[0].Resources.Requests = valkeyCluster.Spec.Resources.Requests
				ss.Spec.Template.Spec.Containers[0].Resources.Limits = valkeyCluster.Spec.Resources.Limits
				return ss
			})
			if err != nil {
				log.Error(err, fmt.Sprintf("Failed to update statefulset %s/%s requests and limits", valkeyCluster.Namespace, stsName))
				return &ctrl.Result{}, err
			}
			r.Recorder.Event(valkeyCluster, "Normal", "Updated",
				fmt.Sprintf("StatefulSet resources requests %s/%s is updated", found.Namespace, found.Name))
			return &ctrl.Result{Requeue: true}, nil
		} else {
			// for now, we only implement scaling up of CPU and memory
			requestOrLimitChange := false
			var cpuRequest, memoryRequest, cpuLimit, memoryLimit resource.Quantity
			if foundRequests.Cpu().Cmp(*valkeyCluster.Spec.Resources.Requests.Cpu()) == -1 {
				requestOrLimitChange = true
				cpuRequest = *valkeyCluster.Spec.Resources.Requests.Cpu()
			} else {
				cpuRequest = *foundRequests.Cpu()
			}
			if foundRequests.Memory().Cmp(*valkeyCluster.Spec.Resources.Requests.Memory()) == -1 {
				requestOrLimitChange = true
				memoryRequest = *valkeyCluster.Spec.Resources.Requests.Memory()
			} else {
				memoryRequest = *foundRequests.Memory()
			}
			if foundLimits.Cpu().Cmp(*valkeyCluster.Spec.Resources.Limits.Cpu()) == -1 {
				requestOrLimitChange = true
				cpuLimit = *valkeyCluster.Spec.Resources.Limits.Cpu()
			} else {
				cpuLimit = *foundLimits.Cpu()
			}
			if foundLimits.Memory().Cmp(*valkeyCluster.Spec.Resources.Limits.Memory()) == -1 {
				requestOrLimitChange = true
				memoryLimit = *valkeyCluster.Spec.Resources.Limits.Memory()
			} else {
				memoryLimit = *foundLimits.Memory()
			}
			if requestOrLimitChange {
				err := r.updateStatefulSet(ctx, valkeyCluster.Namespace, stsName, func(ss *appsv1.StatefulSet) *appsv1.StatefulSet {
					ss.Spec.Template.Spec.Containers[0].Resources.Requests = corev1.ResourceList{
						corev1.ResourceCPU:    cpuRequest,
						corev1.ResourceMemory: memoryRequest,
					}
					ss.Spec.Template.Spec.Containers[0].Resources.Limits = corev1.ResourceList{
						corev1.ResourceCPU:    cpuLimit,
						corev1.ResourceMemory: memoryLimit,
					}
					return ss
				})
				if err != nil {
					log.Error(err, fmt.Sprintf("Failed to update statefulset %s/%s requests and limits", valkeyCluster.Namespace, stsName))
					return &ctrl.Result{}, err
				}
				return &ctrl.Result{Requeue: true}, nil
			}
		}

		if found.Spec.MinReadySeconds != valkeyCluster.Spec.MinReadySeconds {
			log.Info(fmt.Sprintf("StatefulSet needs to change minReadySeconds from %d to %d", found.Spec.MinReadySeconds, valkeyCluster.Spec.MinReadySeconds))
			err := r.updateStatefulSet(ctx, valkeyCluster.Namespace, stsName, func(ss *appsv1.StatefulSet) *appsv1.StatefulSet {
				ss.Spec.MinReadySeconds = valkeyCluster.Spec.MinReadySeconds
				return ss
			})
			if err != nil {
				log.Error(err, fmt.Sprintf("Failed to update statefulset %s/%s minReadySeconds", valkeyCluster.Namespace, stsName))
				return &ctrl.Result{}, err
			}
			r.Recorder.Event(valkeyCluster, "Normal", "Updated",
				fmt.Sprintf("StatefulSet MinReadySeconds %s/%s is updated", found.Namespace, found.Name))
			log.Info(fmt.Sprintf("StatefulSet %s/%s minReadySeconds updated", valkeyCluster.Namespace, stsName))
			return &ctrl.Result{Requeue: true}, nil
		}
	}
	return nil, nil
}

// Compares actual to desired, and returns true if there is a difference in any of the following:
// - valkey-server image
// - valkey-server command
// - valkey-server lifecycle
// - valkey-server environment
// - valkey-server readiness probe
// - metrics command
// It's really important that you consider updating applyDesiredStatefulSetSpec if this function changes
func (r *ValkeyClusterReconciler) compareActualToDesiredStatefulSet(ctx context.Context, valkeyCluster *cachev1alpha1.ValkeyCluster, stsName string) (bool, error) {
	log := log.FromContext(ctx)
	actual, err := r.findStatefulsetByName(ctx, valkeyCluster.Namespace, stsName)
	if err != nil {
		return false, err
	}

	desired := r.statefulSet(stsName, statefulSetSize(valkeyCluster), valkeyCluster)

	diff := false
	if !cmp.Equal(actual.Spec.Template.Spec.Containers[0].Image, desired.Spec.Template.Spec.Containers[0].Image) {
		log.Info(fmt.Sprintf("StatefulSet %s Image is different: %s", stsName, cmp.Diff(actual.Spec.Template.Spec.Containers[0].Image, desired.Spec.Template.Spec.Containers[0].Image)))
		diff = true
	}
	if !cmp.Equal(actual.Spec.Template.Spec.Containers[0].Command, desired.Spec.Template.Spec.Containers[0].Command) {
		log.Info(fmt.Sprintf("StatefulSet %s Command is different: %s", stsName, cmp.Diff(actual.Spec.Template.Spec.Containers[0].Command, desired.Spec.Template.Spec.Containers[0].Command)))
		diff = true
	}
	if !cmp.Equal(actual.Spec.Template.Spec.Containers[0].Lifecycle, desired.Spec.Template.Spec.Containers[0].Lifecycle) {
		log.Info(fmt.Sprintf("StatefulSet %s Lifecycle is different: %s", stsName, cmp.Diff(actual.Spec.Template.Spec.Containers[0].Lifecycle, desired.Spec.Template.Spec.Containers[0].Lifecycle)))
		diff = true
	}
	if !cmp.Equal(actual.Spec.Template.Spec.Containers[0].Env, desired.Spec.Template.Spec.Containers[0].Env) {
		log.Info(fmt.Sprintf("StatefulSet %s Env is different: %s", stsName, cmp.Diff(actual.Spec.Template.Spec.Containers[0].Env, desired.Spec.Template.Spec.Containers[0].Env)))
		diff = true
	}
	if !cmp.Equal(actual.Spec.Template.Spec.Containers[0].ReadinessProbe, desired.Spec.Template.Spec.Containers[0].ReadinessProbe) {
		log.Info(fmt.Sprintf("StatefulSet %s ReadinessProbe is different: %s", stsName, cmp.Diff(actual.Spec.Template.Spec.Containers[0].ReadinessProbe, desired.Spec.Template.Spec.Containers[0].ReadinessProbe)))
		diff = true
	}

	// metrics command, this will detect if auth has been enabled
	if !cmp.Equal(actual.Spec.Template.Spec.Containers[1].Args, desired.Spec.Template.Spec.Containers[1].Args) {
		log.Info(fmt.Sprintf("StatefulSet %s metrics command is different: %s", stsName, cmp.Diff(actual.Spec.Template.Spec.Containers[1].Args, desired.Spec.Template.Spec.Containers[1].Args)))
		diff = true
	}

	return diff, nil
}

// It's really important that you consider updating compareActualToDesiredStatefulSet if this function changes
func (r *ValkeyClusterReconciler) applyDesiredStatefulSetSpec(valkeyCluster *cachev1alpha1.ValkeyCluster, stsName string) func(ss *appsv1.StatefulSet) *appsv1.StatefulSet {
	desired := r.statefulSet(stsName, statefulSetSize(valkeyCluster), valkeyCluster)
	return func(ss *appsv1.StatefulSet) *appsv1.StatefulSet {
		ss.Spec.Template.Spec.Containers[0].Command = desired.Spec.Template.Spec.Containers[0].Command
		ss.Spec.Template.Spec.Containers[0].Lifecycle = desired.Spec.Template.Spec.Containers[0].Lifecycle
		ss.Spec.Template.Spec.Containers[0].Env = desired.Spec.Template.Spec.Containers[0].Env
		ss.Spec.Template.Spec.Containers[0].Image = desired.Spec.Template.Spec.Containers[0].Image
		ss.Spec.Template.Spec.Containers[0].ReadinessProbe = desired.Spec.Template.Spec.Containers[0].ReadinessProbe

		ss.Spec.Template.Spec.Containers[1].Args = desired.Spec.Template.Spec.Containers[1].Args
		ss.Spec.Template.Spec.Containers[1].Image = desired.Spec.Template.Spec.Containers[1].Image
		return ss
	}
}
