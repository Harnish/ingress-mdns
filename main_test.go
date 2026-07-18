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
