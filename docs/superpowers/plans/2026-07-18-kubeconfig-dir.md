# Kubeconfig Directory Support Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let `ingress-mdns` watch ingresses across multiple clusters by pointing `--kubeconfig-dir` at a directory of kubeconfig files, one per cluster.

**Architecture:** A new `clusterManager` polls a directory every 10s with stdlib `os.ReadDir`, diffs the file list against what's currently running, and starts/stops one `watchIngresses` goroutine per file. Registry keys gain a cluster-label prefix (derived from filename) so entries from different clusters can't collide. `--kubeconfig-dir` is mutually exclusive with the existing `--kubeconfig` flag; single-cluster mode is unchanged except its entries now use the label `"default"`.

**Tech Stack:** Go, `k8s.io/client-go` (`clientcmd`, `kubernetes`), stdlib only for directory polling (no fsnotify).

## Global Constraints

- `--kubeconfig` and `--kubeconfig-dir` are mutually exclusive; exactly one must be set (spec: "Flag behavior").
- Directory polling is stdlib-only, no fsnotify dependency; interval hardcoded to 10s, not a flag (spec: "Directory polling", "Out of scope").
- Cluster label = kubeconfig filename with extension stripped, e.g. `prod.yaml` -> `prod` (spec: "Directory polling").
- Registry keys become `cluster/namespace/name`; the manual-file entry's key stays `"manual"`, unaffected by clustering (spec: "Registry / key changes").
- Bad/unreadable kubeconfig file: log once, retry every poll tick, never abort the process (spec: "Error handling").
- Missing/unreadable `--kubeconfig-dir` at startup is fatal, same severity as today's missing `--kubeconfig` (spec: "Flag behavior", "Error handling").

---

### Task 1: `ServiceRegistry.removeCluster`

**Files:**
- Modify: `main.go` (add method after `remove`, main.go:101)
- Create: `main_test.go`

**Interfaces:**
- Produces: `(r *ServiceRegistry) removeCluster(label string)` — shuts down and deletes every registry entry whose key is prefixed `label + "/"`. Used by `clusterManager` in Task 3.

- [ ] **Step 1: Write the failing test**

Create `main_test.go`:

```go
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./... -run TestServiceRegistryRemoveCluster -v`
Expected: FAIL — build error, `registry.removeCluster` undefined.

- [ ] **Step 3: Implement `removeCluster`**

In `main.go`, immediately after the existing `remove` method (main.go:92-101), add:

```go
func (r *ServiceRegistry) removeCluster(label string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	prefix := label + "/"
	for key, svcs := range r.services {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		for _, svc := range svcs {
			if svc.Server != nil {
				svc.Server.Shutdown()
			}
		}
		delete(r.services, key)
	}
}
```

`strings` is already imported in `main.go`.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./... -run TestServiceRegistryRemoveCluster -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add main.go main_test.go
git commit -m "feat: add ServiceRegistry.removeCluster for multi-cluster teardown"
```

---

### Task 2: Thread `clusterLabel` through ingress keys and watch loop

**Files:**
- Modify: `main.go` — `ingressKey` (main.go:249-251), `watchIngresses` (main.go:165), `drainWatchEvents` (main.go:220), the single call site in `main()` (main.go:151)
- Modify: `main_test.go`

**Interfaces:**
- Consumes: nothing new.
- Produces: `ingressKey(clusterLabel string, ingress *networkingv1.Ingress) string`, `watchIngresses(ctx context.Context, clientset *kubernetes.Clientset, registry *ServiceRegistry, clusterLabel string)`, `drainWatchEvents(ctx context.Context, ch <-chan watch.Event, registry *ServiceRegistry, clusterLabel string) bool`. Task 3's `clusterManager` calls `watchIngresses` with a per-cluster label; Task 5's single-cluster branch calls it with `"default"`.

This task is one commit because Go requires the whole package to compile — the signature change and all call sites must land together.

- [ ] **Step 1: Write the failing test**

Append to `main_test.go`:

```go
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
```

Add imports to `main_test.go`:

```go
import (
	"testing"

	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./... -run TestIngressKeyIncludesClusterLabel -v`
Expected: FAIL — build error, `ingressKey` called with 1 argument, wants 2 (current signature only takes `ingress`).

- [ ] **Step 3: Update `ingressKey`, `watchIngresses`, `drainWatchEvents`, and the call site**

Replace `ingressKey` (main.go:249-251):

```go
func ingressKey(clusterLabel string, ingress *networkingv1.Ingress) string {
	return clusterLabel + "/" + ingress.Namespace + "/" + ingress.Name
}
```

Replace `watchIngresses` (main.go:165-216):

```go
func watchIngresses(ctx context.Context, clientset *kubernetes.Clientset, registry *ServiceRegistry, clusterLabel string) {
	for {
		if ctx.Err() != nil {
			return
		}

		list, err := clientset.NetworkingV1().Ingresses("").List(ctx, metav1.ListOptions{})
		if err != nil {
			log.Printf("[%s] failed to list ingresses: %v; retrying in 5s", clusterLabel, err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
				continue
			}
		}

		for i := range list.Items {
			ingress := &list.Items[i]
			registry.update(ingressKey(clusterLabel, ingress), processIngress(ingress))
		}

		log.Printf("[%s] Watching ingresses (resourceVersion %s)", clusterLabel, list.ResourceVersion)

		watcher, err := clientset.NetworkingV1().Ingresses("").Watch(ctx, metav1.ListOptions{
			ResourceVersion: list.ResourceVersion,
		})
		if err != nil {
			log.Printf("[%s] failed to start watch: %v; retrying in 5s", clusterLabel, err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
				continue
			}
		}

		ctxDone := drainWatchEvents(ctx, watcher.ResultChan(), registry, clusterLabel)
		watcher.Stop()

		if ctxDone {
			return
		}

		log.Printf("[%s] Watch ended, re-listing...", clusterLabel)
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}
```

Replace `drainWatchEvents` (main.go:220-247):

```go
func drainWatchEvents(ctx context.Context, ch <-chan watch.Event, registry *ServiceRegistry, clusterLabel string) bool {
	for {
		select {
		case <-ctx.Done():
			return true
		case event, ok := <-ch:
			if !ok {
				return false
			}
			if event.Type == watch.Error {
				log.Printf("[%s] Watch error event: %v", clusterLabel, event.Object)
				return false
			}
			ingress, ok := event.Object.(*networkingv1.Ingress)
			if !ok {
				continue
			}
			key := ingressKey(clusterLabel, ingress)
			switch event.Type {
			case watch.Added, watch.Modified:
				registry.update(key, processIngress(ingress))
			case watch.Deleted:
				log.Printf("[%s] Ingress deleted: %s", clusterLabel, key)
				registry.remove(key)
			}
		}
	}
}
```

In `main()` (main.go:151), update the single existing call site:

```go
	go watchIngresses(ctx, clientset, registry, "default")
```

- [ ] **Step 4: Run tests to verify they pass, and full build**

Run: `go build ./... && go test ./... -v`
Expected: PASS, no build errors.

- [ ] **Step 5: Commit**

```bash
git add main.go main_test.go
git commit -m "feat: thread cluster label through ingress keys and watch loop"
```

---

### Task 3: `clusterManager` — directory diff, start/stop, skip-invalid

**Files:**
- Create: `cluster_manager.go`
- Create: `cluster_manager_test.go`

**Interfaces:**
- Consumes: `ServiceRegistry.removeCluster(label string)` (Task 1), `watchIngresses(ctx, clientset, registry, clusterLabel)` (Task 2).
- Produces: `newClusterManager(dir string, registry *ServiceRegistry) *clusterManager`, `(m *clusterManager) poll(ctx context.Context)`, `clusterLabel(path string) string`. Task 4 adds `run()` on top of `poll`. Task 5's `main()` calls `newClusterManager` and (via Task 4) `.run(ctx)`.

- [ ] **Step 1: Write the failing tests**

Create `cluster_manager_test.go`:

```go
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./... -run TestClusterManager -v` and `go test ./... -run TestClusterLabel -v`
Expected: FAIL — build error, `newClusterManager` / `clusterLabel` undefined.

- [ ] **Step 3: Implement `cluster_manager.go`**

```go
package main

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"strings"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

// clusterManager watches a directory of kubeconfig files and starts/stops
// one watchIngresses goroutine per file found. It is single-goroutine: all
// methods are called from run() (added in a later task), so no locking is
// needed on its maps.
type clusterManager struct {
	dir        string
	registry   *ServiceRegistry
	running    map[string]context.CancelFunc
	lastFailed map[string]bool
}

func newClusterManager(dir string, registry *ServiceRegistry) *clusterManager {
	return &clusterManager{
		dir:        dir,
		registry:   registry,
		running:    make(map[string]context.CancelFunc),
		lastFailed: make(map[string]bool),
	}
}

// poll reads the directory once, starting a cluster watcher for any new
// file and stopping/cleaning up any that disappeared since the last poll.
func (m *clusterManager) poll(ctx context.Context) {
	entries, err := os.ReadDir(m.dir)
	if err != nil {
		log.Printf("failed to read kubeconfig dir %s: %v", m.dir, err)
		return
	}

	seen := make(map[string]bool, len(entries))

	for _, entry := range entries {
		if entry.IsDir() || strings.HasPrefix(entry.Name(), ".") {
			continue
		}

		path := filepath.Join(m.dir, entry.Name())
		seen[path] = true

		if _, ok := m.running[path]; ok {
			continue
		}

		m.startCluster(ctx, path)
	}

	for path, cancel := range m.running {
		if seen[path] {
			continue
		}

		cancel()
		delete(m.running, path)
		delete(m.lastFailed, path)

		label := clusterLabel(path)
		m.registry.removeCluster(label)
		log.Printf("kubeconfig removed, stopped watching cluster %s (%s)", label, path)
	}
}

func (m *clusterManager) startCluster(ctx context.Context, path string) {
	config, err := clientcmd.BuildConfigFromFlags("", path)
	if err == nil {
		var clientset *kubernetes.Clientset
		clientset, err = kubernetes.NewForConfig(config)
		if err == nil {
			label := clusterLabel(path)
			clusterCtx, cancel := context.WithCancel(ctx)
			m.running[path] = cancel
			delete(m.lastFailed, path)

			log.Printf("watching new cluster %s (%s)", label, path)
			go watchIngresses(clusterCtx, clientset, m.registry, label)
			return
		}
	}

	if !m.lastFailed[path] {
		log.Printf("failed to load kubeconfig %s: %v", path, err)
		m.lastFailed[path] = true
	}
}

// clusterLabel derives a cluster's registry/log label from its kubeconfig
// file name, e.g. "prod.yaml" -> "prod".
func clusterLabel(path string) string {
	base := filepath.Base(path)
	return strings.TrimSuffix(base, filepath.Ext(base))
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./... -v`
Expected: PASS (all tests, including Tasks 1-2's).

- [ ] **Step 5: Commit**

```bash
git add cluster_manager.go cluster_manager_test.go
git commit -m "feat: add clusterManager to poll a kubeconfig directory"
```

---

### Task 4: `clusterManager.run` — polling loop

**Files:**
- Modify: `cluster_manager.go`
- Modify: `cluster_manager_test.go`

**Interfaces:**
- Consumes: `(m *clusterManager) poll(ctx context.Context)` (Task 3).
- Produces: `(m *clusterManager) run(ctx context.Context)` — polls immediately, then every `m.pollInterval`, until `ctx` is cancelled. Task 5's `main()` calls this in a goroutine.

- [ ] **Step 1: Write the failing test**

Append to `cluster_manager_test.go`:

```go
func TestClusterManagerRunReturnsWhenContextCancelled(t *testing.T) {
	dir := t.TempDir()
	registry := newServiceRegistry()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before run() starts

	m := newClusterManager(dir, registry)

	done := make(chan struct{})
	go func() {
		m.run(ctx)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("expected run() to return promptly when ctx is already cancelled")
	}
}
```

Add `"time"` to the test file's imports.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./... -run TestClusterManagerRunReturnsWhenContextCancelled -v`
Expected: FAIL — build error, `m.run` undefined.

- [ ] **Step 3: Implement `run`**

In `cluster_manager.go`, add the import `"time"`, a package-level const, a new struct field, and the `run` method:

```go
import (
	"context"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

const clusterPollInterval = 10 * time.Second
```

Add `pollInterval time.Duration` to the `clusterManager` struct, and set it in `newClusterManager`:

```go
type clusterManager struct {
	dir          string
	registry     *ServiceRegistry
	pollInterval time.Duration
	running      map[string]context.CancelFunc
	lastFailed   map[string]bool
}

func newClusterManager(dir string, registry *ServiceRegistry) *clusterManager {
	return &clusterManager{
		dir:          dir,
		registry:     registry,
		pollInterval: clusterPollInterval,
		running:      make(map[string]context.CancelFunc),
		lastFailed:   make(map[string]bool),
	}
}
```

Add, after `newClusterManager`:

```go
// run polls the directory immediately, then on every pollInterval tick,
// until ctx is cancelled.
func (m *clusterManager) run(ctx context.Context) {
	m.poll(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(m.pollInterval):
			m.poll(ctx)
		}
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./... -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add cluster_manager.go cluster_manager_test.go
git commit -m "feat: add clusterManager.run polling loop"
```

---

### Task 5: Wire `--kubeconfig-dir` into `main()`

**Files:**
- Modify: `main.go` — flags and body of `main()` (main.go:1-32 header comment, main.go:116-161)
- Modify: `main_test.go`

**Interfaces:**
- Consumes: `validateKubeconfigFlags` (new, this task), `newClusterManager` + `.run(ctx)` (Tasks 3-4), `watchIngresses(ctx, clientset, registry, "default")` (Task 2, single-cluster branch).
- Produces: `validateKubeconfigFlags(kubeconfig, kubeconfigDir string) error`.

- [ ] **Step 1: Write the failing test**

Append to `main_test.go`:

```go
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./... -run TestValidateKubeconfigFlags -v`
Expected: FAIL — build error, `validateKubeconfigFlags` undefined.

- [ ] **Step 3: Implement `validateKubeconfigFlags` and wire up `main()`**

Add, above `func main()`:

```go
func validateKubeconfigFlags(kubeconfig, kubeconfigDir string) error {
	if kubeconfig == "" && kubeconfigDir == "" {
		return fmt.Errorf("either --kubeconfig or --kubeconfig-dir is required")
	}
	if kubeconfig != "" && kubeconfigDir != "" {
		return fmt.Errorf("--kubeconfig and --kubeconfig-dir are mutually exclusive")
	}
	return nil
}
```

`fmt` is already imported in `main.go`.

Replace `main()` (main.go:116-161) in full:

```go
func main() {
	var kubeconfig string
	var kubeconfigDir string
	var manualFile string

	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to kubeconfig")
	flag.StringVar(&kubeconfigDir, "kubeconfig-dir", "", "Path to directory of kubeconfig files, one per cluster (mutually exclusive with --kubeconfig)")
	flag.StringVar(&manualFile, "manual", "", "Path to manual JSON file")
	flag.Parse()

	if err := validateKubeconfigFlags(kubeconfig, kubeconfigDir); err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	registry := newServiceRegistry()

	if manualFile != "" {
		manualServices, err := processManualFile(manualFile)
		if err != nil {
			log.Fatalf("failed to process manual file: %v", err)
		}
		registry.update("manual", manualServices)
	}

	if kubeconfigDir != "" {
		info, err := os.Stat(kubeconfigDir)
		if err != nil || !info.IsDir() {
			log.Fatalf("--kubeconfig-dir %s is not a readable directory", kubeconfigDir)
		}

		manager := newClusterManager(kubeconfigDir, registry)
		go manager.run(ctx)
	} else {
		config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			log.Fatalf("failed to load kubeconfig: %v", err)
		}

		clientset, err := kubernetes.NewForConfig(config)
		if err != nil {
			log.Fatalf("failed to create k8s client: %v", err)
		}

		go watchIngresses(ctx, clientset, registry, "default")
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down...")
	cancel()
	registry.shutdownAll()
	log.Println("Done")
}
```

Update the file header usage comment (main.go:6-9) to document the new flag:

```go
// Usage:
//   ./ingress-mdns \
//     --kubeconfig ~/.kube/config \
//     --manual ./manual.json
//
// Or, for multiple clusters:
//   ./ingress-mdns \
//     --kubeconfig-dir /etc/ingress-mdns/kubeconfigs \
//     --manual ./manual.json
```

- [ ] **Step 4: Run tests to verify they pass, and full build**

Run: `go build ./... && go vet ./... && go test ./... -v`
Expected: PASS, no build/vet errors.

- [ ] **Step 5: Commit**

```bash
git add main.go main_test.go
git commit -m "feat: wire --kubeconfig-dir into main()"
```

---

### Task 6: Manual build and smoke verification

**Files:** none (verification only)

**Interfaces:** none — exercises the full binary built in Tasks 1-5.

- [ ] **Step 1: Build the binary**

Run: `go build -o /tmp/ingress-mdns .`
Expected: builds with no errors.

- [ ] **Step 2: Verify flag validation**

Run: `/tmp/ingress-mdns`
Expected: exits immediately with `either --kubeconfig or --kubeconfig-dir is required`.

Run: `/tmp/ingress-mdns --kubeconfig ~/.kube/config --kubeconfig-dir /tmp`
Expected: exits immediately with `--kubeconfig and --kubeconfig-dir are mutually exclusive`.

- [ ] **Step 3: Verify directory mode picks up a valid and an invalid kubeconfig**

```bash
mkdir -p /tmp/kcdir
cp ~/.kube/config /tmp/kcdir/home.yaml
echo 'not: [valid kubeconfig' > /tmp/kcdir/bad.yaml
/tmp/ingress-mdns --kubeconfig-dir /tmp/kcdir
```

Expected log lines within a few seconds:
- `watching new cluster home (/tmp/kcdir/home.yaml)`
- `failed to load kubeconfig /tmp/kcdir/bad.yaml: ...` (logged once, not repeated every 10s)
- `[home] Watching ingresses (resourceVersion ...)`

- [ ] **Step 4: Verify removing a file stops its cluster**

While the process from Step 3 is still running, in another terminal:

```bash
rm /tmp/kcdir/home.yaml
```

Expected within ~10s: `kubeconfig removed, stopped watching cluster home (/tmp/kcdir/home.yaml)`

Stop the process with Ctrl-C; expect `Shutting down...` then `Done`.

- [ ] **Step 5: Clean up**

```bash
rm -rf /tmp/kcdir /tmp/ingress-mdns
```
