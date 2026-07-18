package main

import (
	"testing"

	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestServiceRegistryRemoveCluster(t *testing.T) {
	registry := newServiceRegistry()
	registry.services["prod/default/app"] = []PublishedService{{Name: "app.local"}}
	registry.services["staging/default/app"] = []PublishedService{{Name: "app.local"}}

	registry.removeCluster("prod")

	if _, ok := registry.services["prod/default/app"]; ok {
		t.Fatal("expected prod/default/app to be removed")
	}
	if _, ok := registry.services["staging/default/app"]; !ok {
		t.Fatal("expected staging/default/app to remain")
	}
}

func TestIngressKeyIncludesClusterLabel(t *testing.T) {
	ingress := &networkingv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "app",
		},
	}

	got := ingressKey("prod", ingress)
	want := "prod/default/app"

	if got != want {
		t.Fatalf("ingressKey() = %q, want %q", got, want)
	}
}

func TestValidateKubeconfigFlags(t *testing.T) {
	tests := []struct {
		name          string
		kubeconfig    string
		kubeconfigDir string
		wantErr       bool
	}{
		{name: "both empty", kubeconfig: "", kubeconfigDir: "", wantErr: true},
		{name: "both set", kubeconfig: "a", kubeconfigDir: "b", wantErr: true},
		{name: "kubeconfig only", kubeconfig: "a", kubeconfigDir: "", wantErr: false},
		{name: "dir only", kubeconfig: "", kubeconfigDir: "b", wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateKubeconfigFlags(tt.kubeconfig, tt.kubeconfigDir)
			if (err != nil) != tt.wantErr {
				t.Fatalf("validateKubeconfigFlags(%q, %q) error = %v, wantErr %v", tt.kubeconfig, tt.kubeconfigDir, err, tt.wantErr)
			}
		})
	}
}
