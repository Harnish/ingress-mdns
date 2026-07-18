package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

const testKubeconfig = `apiVersion: v1
kind: Config
clusters:
- cluster:
    server: https://127.0.0.1:1
    insecure-skip-tls-verify: true
  name: test
contexts:
- context:
    cluster: test
    user: test
  name: test
current-context: test
users:
- name: test
  user: {}
`

func writeTestFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("failed to write %s: %v", path, err)
	}
	return path
}

func TestClusterManagerPollStartsAndStopsClusters(t *testing.T) {
	dir := t.TempDir()
	prodPath := writeTestFile(t, dir, "prod.yaml", testKubeconfig)

	registry := newServiceRegistry()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := newClusterManager(dir, registry)
	m.poll(ctx)

	if _, ok := m.running[prodPath]; !ok {
		t.Fatalf("expected %s to be running after first poll", prodPath)
	}
	if len(m.running) != 1 {
		t.Fatalf("expected 1 running cluster, got %d", len(m.running))
	}

	stagingPath := writeTestFile(t, dir, "staging.yaml", testKubeconfig)
	m.poll(ctx)

	if _, ok := m.running[stagingPath]; !ok {
		t.Fatalf("expected %s to be running after second poll", stagingPath)
	}
	if len(m.running) != 2 {
		t.Fatalf("expected 2 running clusters, got %d", len(m.running))
	}

	if err := os.Remove(prodPath); err != nil {
		t.Fatalf("failed to remove %s: %v", prodPath, err)
	}
	m.poll(ctx)

	if _, ok := m.running[prodPath]; ok {
		t.Fatalf("expected %s to be stopped after removal", prodPath)
	}
	if len(m.running) != 1 {
		t.Fatalf("expected 1 running cluster after removal, got %d", len(m.running))
	}
}

func TestClusterManagerPollSkipsInvalidKubeconfig(t *testing.T) {
	dir := t.TempDir()
	badPath := writeTestFile(t, dir, "bad.yaml", "not: [valid kubeconfig")

	registry := newServiceRegistry()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := newClusterManager(dir, registry)
	m.poll(ctx)

	if _, ok := m.running[badPath]; ok {
		t.Fatal("expected invalid kubeconfig to not be started")
	}
	if !m.lastFailed[badPath] {
		t.Fatal("expected invalid kubeconfig to be tracked as failed")
	}
}

func TestClusterLabel(t *testing.T) {
	got := clusterLabel("/etc/kubeconfigs/prod.yaml")
	want := "prod"
	if got != want {
		t.Fatalf("clusterLabel() = %q, want %q", got, want)
	}
}
