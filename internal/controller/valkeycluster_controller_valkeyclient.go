package controller

import (
	"context"
	"fmt"
	"strings"

	cachev1alpha1 "github.com/halter/valkey-cluster-operator/api/v1alpha1"
	"github.com/valkey-io/valkey-go"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// instantiates the client and performs auth if required
func (r *ValkeyClusterReconciler) NewValkeyClient(ctx context.Context, valkeyCluster *cachev1alpha1.ValkeyCluster, ip string, port int) (valkey.Client, error) {
	log := log.FromContext(ctx)
	clientOpts := valkey.ClientOption{InitAddress: []string{fmt.Sprintf("%s:%d", ip, port)}, ForceSingleClient: true}
	client, err := valkey.NewClient(clientOpts)
	if err != nil {
		if strings.Contains(err.Error(), "NOAUTH") {
			clientOpts.Password = valkeyCluster.Spec.Password
		}
		client, err := valkey.NewClient(clientOpts)
		if err != nil {
			log.Error(err, fmt.Sprintf("Could not create client %s/%s (%s:%d)", valkeyCluster.Namespace, valkeyCluster.Name, ip, port))
			return nil, err
		}
		return client, nil
	}
	return client, nil
}
