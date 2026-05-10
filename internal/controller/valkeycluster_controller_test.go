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
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cachev1alpha1 "github.com/halter/valkey-cluster-operator/api/v1alpha1"
)

var _ = Describe("getValkeyConfigContent", func() {
	param := func(name, value string) cachev1alpha1.ValkeyConfigParameter {
		return cachev1alpha1.ValkeyConfigParameter{Name: name, Value: value}
	}
	cluster := func(password string, cfg *cachev1alpha1.ValkeyConfig) *cachev1alpha1.ValkeyCluster {
		return &cachev1alpha1.ValkeyCluster{
			Spec: cachev1alpha1.ValkeyClusterSpec{
				Password:    password,
				ValkeyConfig: cfg,
			},
		}
	}

	It("returns base config when no ValkeyConfig is set", func() {
		out, err := getValkeyConfigContent(cluster("", nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(ContainSubstring("cluster-enabled yes"))
		Expect(out).NotTo(ContainSubstring("maxmemory"))
	})

	It("appends a simple parameter after base config", func() {
		out, err := getValkeyConfigContent(cluster("", &cachev1alpha1.ValkeyConfig{
			Parameters: []cachev1alpha1.ValkeyConfigParameter{param("maxmemory", "100mb")},
		}))
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(ContainSubstring("cluster-enabled yes"))
		Expect(out).To(ContainSubstring("\nmaxmemory 100mb"))
	})

	It("appends a multi-argument parameter", func() {
		out, err := getValkeyConfigContent(cluster("", &cachev1alpha1.ValkeyConfig{
			Parameters: []cachev1alpha1.ValkeyConfigParameter{param("save", "900 1")},
		}))
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(ContainSubstring("\nsave 900 1"))
	})

	It("appends a quoted value verbatim", func() {
		out, err := getValkeyConfigContent(cluster("", &cachev1alpha1.ValkeyConfig{
			Parameters: []cachev1alpha1.ValkeyConfigParameter{param("requirepass", `"my secret"`)},
		}))
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(ContainSubstring(`requirepass "my secret"`))
	})

	It("appends multiple parameters in specified order", func() {
		out, err := getValkeyConfigContent(cluster("", &cachev1alpha1.ValkeyConfig{
			Parameters: []cachev1alpha1.ValkeyConfigParameter{
				param("maxmemory", "512mb"),
				param("maxmemory-policy", "allkeys-lru"),
				param("maxmemory-clients", "10mb"),
			},
		}))
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(ContainSubstring("\nmaxmemory 512mb"))
		Expect(out).To(ContainSubstring("\nmaxmemory-policy allkeys-lru"))
		Expect(out).To(ContainSubstring("\nmaxmemory-clients 10mb"))
		// Order is preserved
		maxIdx := strings.Index(out, "maxmemory 512mb")
		policyIdx := strings.Index(out, "maxmemory-policy allkeys-lru")
		clientsIdx := strings.Index(out, "maxmemory-clients 10mb")
		Expect(maxIdx).To(BeNumerically("<", policyIdx))
		Expect(policyIdx).To(BeNumerically("<", clientsIdx))
	})

	It("supports repeated keys for multi-value directives", func() {
		out, err := getValkeyConfigContent(cluster("", &cachev1alpha1.ValkeyConfig{
			Parameters: []cachev1alpha1.ValkeyConfigParameter{
				param("save", "900 1"),
				param("save", "300 10"),
			},
		}))
		Expect(err).NotTo(HaveOccurred())
		Expect(strings.Count(out, "\nsave ")).To(Equal(2))
		Expect(out).To(ContainSubstring("\nsave 900 1"))
		Expect(out).To(ContainSubstring("\nsave 300 10"))
		Expect(strings.Index(out, "save 900 1")).To(BeNumerically("<", strings.Index(out, "save 300 10")))
	})

	It("parameters override operator-injected password (last-wins)", func() {
		out, err := getValkeyConfigContent(cluster("operatorpass", &cachev1alpha1.ValkeyConfig{
			Parameters: []cachev1alpha1.ValkeyConfigParameter{param("requirepass", "custom")},
		}))
		Expect(err).NotTo(HaveOccurred())
		// Operator injects requirepass operatorpass; user param requirepass custom comes after
		operatorIdx := strings.Index(out, "requirepass operatorpass")
		userIdx := strings.Index(out, "requirepass custom")
		Expect(operatorIdx).To(BeNumerically(">=", 0))
		Expect(userIdx).To(BeNumerically(">", operatorIdx))
	})

	It("returns RawConfig verbatim when set", func() {
		raw := "port 6379\ncluster-enabled yes\n"
		out, err := getValkeyConfigContent(cluster("", &cachev1alpha1.ValkeyConfig{RawConfig: raw}))
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(Equal(raw))
	})

	It("appends Parameters on top of RawConfig when both are set", func() {
		raw := "port 6379\ncluster-enabled yes\n"
		out, err := getValkeyConfigContent(cluster("", &cachev1alpha1.ValkeyConfig{
			RawConfig:  raw,
			Parameters: []cachev1alpha1.ValkeyConfigParameter{param("maxmemory", "64mb")},
		}))
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(HavePrefix(raw))
		Expect(out).To(ContainSubstring("\nmaxmemory 64mb"))
	})
})

var _ = Describe("ValkeyCluster Controller", func() {
	Context("When reconciling a resource", func() {
		const resourceName = "test-resource"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default", // TODO(user):Modify as needed
		}
		valkeycluster := &cachev1alpha1.ValkeyCluster{}

		BeforeEach(func() {
			By("creating the custom resource for the Kind ValkeyCluster")
			err := k8sClient.Get(ctx, typeNamespacedName, valkeycluster)
			if err != nil && errors.IsNotFound(err) {
				resource := &cachev1alpha1.ValkeyCluster{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: "default",
					},
					// TODO(user): Specify other spec details if needed.
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			// TODO(user): Cleanup logic after each test, like removing the resource instance.
			resource := &cachev1alpha1.ValkeyCluster{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance ValkeyCluster")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		})

		It("should successfully reconcile the resource", func() {
			By("Reconciling the created resource")
			controllerReconciler := &ValkeyClusterReconciler{
				Client:   k8sClient,
				Scheme:   k8sClient.Scheme(),
				Recorder: record.NewFakeRecorder(3),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
			// TODO(user): Add more specific assertions depending on your controller's reconciliation logic.
			// Example: If you expect a certain status condition after reconciliation, verify it here.
		})
	})
})
