package main

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

// clusterManager watches a directory of kubeconfig files and starts/stops
// one watchIngresses goroutine per file found. It is single-goroutine: all
// methods are called from run(), so no locking is needed on its maps.
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
		log.Printf("kubeconfig removed, stopped watching cluster %s (%s)", label, path)
	}

	for path := range m.lastFailed {
		if seen[path] {
			continue
		}
		delete(m.lastFailed, path)
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
			go func() {
				watchIngresses(clusterCtx, clientset, m.registry, label)
				m.registry.removeCluster(label)
			}()
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
