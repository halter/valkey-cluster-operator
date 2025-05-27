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

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// ValkeyClusterSpec defines the desired state of ValkeyCluster
type ValkeyClusterSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// +kubebuilder:validation:Minimum=1
	// Shards defines the number of shards in the valkey cluster
	// +operator-sdk:csv:customresourcedefinitions:type=spec
	Shards int32 `json:"shards,omitempty"`

	// +kubebuilder:validation:Minimum=0
	// Replicas defines the number of replicas per shard in the cluster
	// +operator-sdk:csv:customresourcedefinitions:type=spec
	Replicas int32 `json:"replicas,omitempty"`

	// Valkey docker image to use
	// +operator-sdk:csv:customresourcedefinitions:type=spec
	Image string `json:"image,omitempty"`

	// Node selector
	// +operator-sdk:csv:customresourcedefinitions:type=spec
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// Resources requirements and limits for the containers
	// +operator-sdk:csv:customresourcedefinitions:type=spec
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`

	// Valkey pod storage
	// +operator-sdk:csv:customresourcedefinitions:type=spec
	Storage *corev1.PersistentVolumeClaimSpec `json:"storage,omitempty"`

	// Tolerations
	// +operator-sdk:csv:customresourcedefinitions:type=spec
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`

	// Topology keys to use in StatefulSet Pod antiaffinity used to ensure that pods in a shard are run on separate hosts or separate availability zones
	// +operator-sdk:csv:customresourcedefinitions:type=spec
	AntiAffinityTopologyKeys []string `json:"antiAffinityTopologyKeys,omitempty"`
}

// ValkeyClusterStatus defines the observed state of ValkeyCluster
type ValkeyClusterStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// Conditions store the status conditions of the ValkeyCluster instances
	// +operator-sdk:csv:customresourcedefinitions:type=status
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,1,rep,name=conditions"`

	// Information about each pod
	// +operator-sdk:csv:customresourcedefinitions:type=status
	ClusterNodes map[string][]ValkeyClusterNode `json:"cluster_nodes,omitempty"`
}

type ValkeyClusterNode struct {
	Pod          string   `json:"pod,omitempty"`
	IP           string   `json:"ip,omitempty"`
	ID           string   `json:"id,omitempty"`
	MasterNodeID string   `json:"master_node_id,omitempty"`
	SlotRange    string   `json:"slot_range,omitempty"`
	Flags        []string `json:"flags,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// ValkeyCluster is the Schema for the valkeyclusters API
type ValkeyCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ValkeyClusterSpec   `json:"spec,omitempty"`
	Status ValkeyClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ValkeyClusterList contains a list of ValkeyCluster
type ValkeyClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ValkeyCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ValkeyCluster{}, &ValkeyClusterList{})
}
