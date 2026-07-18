package main

import "testing"

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
