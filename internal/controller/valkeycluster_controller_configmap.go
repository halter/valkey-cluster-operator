package controller

import (
	"context"
	"crypto/sha256"
	"fmt"

	cachev1alpha1 "github.com/halter/valkey-cluster-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

func (r *ValkeyClusterReconciler) upsertConfigMap(ctx context.Context, valkeyCluster *cachev1alpha1.ValkeyCluster) (string, error) {
	logger := log.FromContext(ctx)
	logger.Info("upserting configmap")

	// TODO: Sanity check user-provided valkey.conf

	preStop, err := scripts.ReadFile("scripts/pre_stop.sh")
	if err != nil {
		logger.Error(err, "failed to read pre_stop.sh")
		return "", err
	}
	postStart, err := scripts.ReadFile("scripts/post_start.sh")
	if err != nil {
		logger.Error(err, "failed to read post_start.sh")
		return "", err
	}
	meet, err := scripts.ReadFile("scripts/meet.sh")
	if err != nil {
		logger.Error(err, "failed to read meet.sh")
		return "", err
	}
	utils, err := scripts.ReadFile("scripts/utils.sh")
	if err != nil {
		logger.Error(err, "failed to read utils.sh")
		return "", err
	}
	readiness, err := scripts.ReadFile("scripts/readiness.sh")
	if err != nil {
		logger.Error(err, "failed to read readiness.sh")
		return "", err
	}
	ls := labelsForValkeyCluster(valkeyCluster.Name)
	cmData := map[string]string{
		"pre_stop.sh":   string(preStop),
		"post_start.sh": string(postStart),
		"meet.sh":       string(meet),
		"utils.sh":      string(utils),
		"readiness.sh":  string(readiness),
	}
	valkeyConfContent, err := getValkeyConfigContent(valkeyCluster)
	if err != nil {
		return "", fmt.Errorf("failed to read valkey config content: %v", err)
	}
	valkeyConfHash := fmt.Sprintf("%x", sha256.Sum256([]byte(valkeyConfContent)))
	cmData["valkey.conf"] = valkeyConfContent
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      valkeyCluster.Name,
			Namespace: valkeyCluster.Namespace,
			Labels:    ls,
			Annotations: map[string]string{
				// Used to confirm what pods are running what configmaps. Does not influence reconcilation.
				valkeyConfigAnnotation: valkeyConfHash,
			},
		},
		Data: cmData,
	}
	if valkeyCluster.Spec.Password != "" {
		cm.Data["password"] = valkeyCluster.Spec.Password
	}
	if err := controllerutil.SetControllerReference(valkeyCluster, cm, r.Scheme); err != nil {
		return "", err
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
					return "", err
				}
				r.Recorder.Event(valkeyCluster, "Normal", "Updated",
					fmt.Sprintf("ConfigMap %s/%s is updated", valkeyCluster.Namespace, valkeyCluster.Name))
			}
		} else {
			logger.Error(err, "failed to create ConfigMap")
			return "", err
		}
	} else {
		r.Recorder.Event(valkeyCluster, "Normal", "Created",
			fmt.Sprintf("ConfigMap %s/%s is created", valkeyCluster.Namespace, valkeyCluster.Name))
	}
	return valkeyConfHash, nil
}

func getValkeyConfigContent(valkeyCluster *cachev1alpha1.ValkeyCluster) (string, error) {
	if valkeyCluster.Spec.ValkeyConfig != nil && valkeyCluster.Spec.ValkeyConfig.RawConfig != "" {
		return valkeyCluster.Spec.ValkeyConfig.RawConfig, nil
	}
	valkeyConf, err := scripts.ReadFile("scripts/valkey.conf")
	if err != nil {
		return "", err
	}
	if valkeyCluster.Spec.Password != "" {
		valkeyConf = []byte(fmt.Sprintf("%[1]s\nrequirepass %[2]s\nprimaryauth %[2]s", string(valkeyConf), valkeyCluster.Spec.Password))

	}
	return string(valkeyConf), nil
}
