package controller

import (
	cachev1alpha1 "github.com/halter/valkey-cluster-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
)

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
