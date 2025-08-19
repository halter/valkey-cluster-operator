package controller

import (
	"context"
	"fmt"

	cachev1alpha1 "github.com/halter/valkey-cluster-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

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
