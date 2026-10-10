// main.go
//
// Build:
//   go build -o ingress-mdns main.go
//
// Usage:
//   ./ingress-mdns \
//     --kubeconfig ~/.kube/config \
//     --manual ./manual.json
//
// Or, for multiple clusters:
//   ./ingress-mdns \
//     --kubeconfig-dir /etc/ingress-mdns/kubeconfigs \
//     --manual ./manual.json
//
// Example manual.json:
// [
//   {
//     "hostname": "myapp.local",
//     "ip": "10.10.10.25"
//   },
//   {
//     "hostname": "printer.local",
//     "ip": "192.168.1.50"
//   }
// ]
//
// This program:
//   - Watches all Kubernetes ingresses and reconciles mDNS entries on change
//   - Reads additional manual hostname/IP mappings
//   - Publishes mDNS (Bonjour/zeroconf) entries on the local network
//
// Notes:
//   - mDNS normally resolves *.local names
//   - Most OSes will ignore arbitrary non-.local names over mDNS
//   - If ingress has no LB IP, it is skipped unless --ingress-ip is set
//   - This publishes A records as mDNS host announcements

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/grandcat/zeroconf"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

type ManualEntry struct {
	Hostname string `json:"hostname"`
	IP       string `json:"ip"`
}

type entry struct {
	sig     string // desired-state signature; "" forces republish next update
	hosts   []string
	servers []*zeroconf.Server
}

type ServiceRegistry struct {
	mu       sync.Mutex
	services map[string]*entry
}

func newServiceRegistry() *ServiceRegistry {
	return &ServiceRegistry{services: make(map[string]*entry)}
}

// signature returns a stable string for a hostname -> IPs mapping.
func signature(desired map[string][]string) string {
	hosts := make([]string, 0, len(desired))
	for h := range desired {
		hosts = append(hosts, h)
	}
	sort.Strings(hosts)
	var sb strings.Builder
	for _, h := range hosts {
		ips := append([]string(nil), desired[h]...)
		sort.Strings(ips)
		sb.WriteString(h + "=" + strings.Join(ips, ",") + ";")
	}
	return sb.String()
}

func (e *entry) shutdown() {
	for _, srv := range e.servers {
		srv.Shutdown()
	}
}

// owner returns the key (other than except) that already publishes host, or "".
// ponytail: linear scan, add a host->key index if registries get large.
func (r *ServiceRegistry) owner(host, except string) string {
	for k, e := range r.services {
		if k == except {
			continue
		}
		for _, h := range e.hosts {
			if h == host {
				return k
			}
		}
	}
	return ""
}

// claimable drops hosts already published by another key (first claim wins;
// manual entries load first so they always win). dropped reports whether any
// host was refused.
func (r *ServiceRegistry) claimable(key string, desired map[string][]string) (map[string][]string, bool) {
	out := make(map[string][]string, len(desired))
	dropped := false
	for host, ips := range desired {
		if o := r.owner(host, key); o != "" {
			log.Printf("Refusing %s for %s: already published by %s", host, key, o)
			dropped = true
			continue
		}
		out[host] = ips
	}
	return out, dropped
}

// update makes the published servers for key match desired (hostname -> IPs).
// It is a no-op when nothing changed, so re-lists don't send mDNS goodbyes.
// Hostnames already published by a different key are refused.
func (r *ServiceRegistry) update(key string, desired map[string][]string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	desired, dropped := r.claimable(key, desired)
	sig := signature(desired)
	if old, ok := r.services[key]; ok {
		if old.sig == sig {
			return
		}
		old.shutdown()
		delete(r.services, key)
	}
	if len(desired) == 0 {
		return
	}

	e := &entry{sig: sig}
	if dropped {
		e.sig = "" // retry once the other owner goes away
	}
	for host, ips := range desired {
		srv, err := publishMDNS(host, ips)
		if err != nil {
			log.Printf("Failed to publish %s -> %v: %v", host, ips, err)
			e.sig = "" // retry on next update
			continue
		}
		e.servers = append(e.servers, srv)
		e.hosts = append(e.hosts, host)
	}
	r.services[key] = e
}

func (r *ServiceRegistry) remove(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.services[key]; ok {
		e.shutdown()
		delete(r.services, key)
	}
}

// prune removes every key under the cluster label not in keep.
func (r *ServiceRegistry) prune(label string, keep map[string]bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	prefix := label + "/"
	for key, e := range r.services {
		if strings.HasPrefix(key, prefix) && !keep[key] {
			log.Printf("Pruning stale ingress: %s", key)
			e.shutdown()
			delete(r.services, key)
		}
	}
}

func (r *ServiceRegistry) removeCluster(label string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	prefix := label + "/"
	for key, e := range r.services {
		if strings.HasPrefix(key, prefix) {
			e.shutdown()
			delete(r.services, key)
		}
	}
}

func (r *ServiceRegistry) shutdownAll() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for key, e := range r.services {
		e.shutdown()
		delete(r.services, key)
	}
}

func validateKubeconfigFlags(kubeconfig, kubeconfigDir string) error {
	if kubeconfig == "" && kubeconfigDir == "" {
		return fmt.Errorf("either --kubeconfig or --kubeconfig-dir is required")
	}
	if kubeconfig != "" && kubeconfigDir != "" {
		return fmt.Errorf("--kubeconfig and --kubeconfig-dir are mutually exclusive")
	}
	return nil
}

func main() {
	var kubeconfig string
	var kubeconfigDir string
	var manualFile string
	var ingressIP string

	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to kubeconfig")
	flag.StringVar(&kubeconfigDir, "kubeconfig-dir", "", "Path to directory of kubeconfig files, one per cluster (mutually exclusive with --kubeconfig)")
	flag.StringVar(&manualFile, "manual", "", "Path to manual JSON file")
	flag.StringVar(&ingressIP, "ingress-ip", "", "Fallback IP for ingresses with no load-balancer IP")
	flag.Parse()

	if err := validateKubeconfigFlags(kubeconfig, kubeconfigDir); err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	registry := newServiceRegistry()

	if manualFile != "" {
		manual, err := processManualFile(manualFile)
		if err != nil {
			log.Fatalf("failed to process manual file: %v", err)
		}
		registry.update("manual", manual)
	}

	if kubeconfigDir != "" {
		info, err := os.Stat(kubeconfigDir)
		if err != nil || !info.IsDir() {
			log.Fatalf("--kubeconfig-dir %s is not a readable directory", kubeconfigDir)
		}

		manager := newClusterManager(kubeconfigDir, registry)
		manager.ingressIP = ingressIP
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

		go watchIngresses(ctx, clientset, registry, "default", ingressIP)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down...")
	cancel()
	registry.shutdownAll()
	log.Println("Done")
}

// watchIngresses lists all ingresses, publishes their mDNS entries, then
// watches for changes. On disconnect it re-lists to catch any missed events.
func watchIngresses(ctx context.Context, clientset *kubernetes.Clientset, registry *ServiceRegistry, clusterLabel, ingressIP string) {
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

		keep := make(map[string]bool, len(list.Items))
		for i := range list.Items {
			ingress := &list.Items[i]
			key := ingressKey(clusterLabel, ingress)
			keep[key] = true
			registry.update(key, processIngress(ingress, ingressIP))
		}
		registry.prune(clusterLabel, keep)

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

		ctxDone := drainWatchEvents(ctx, watcher.ResultChan(), registry, clusterLabel, ingressIP)
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

// drainWatchEvents processes events until the channel closes or ctx is done.
// Returns true if ctx was cancelled, false if the watch channel closed.
func drainWatchEvents(ctx context.Context, ch <-chan watch.Event, registry *ServiceRegistry, clusterLabel, ingressIP string) bool {
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
				registry.update(key, processIngress(ingress, ingressIP))
			case watch.Deleted:
				log.Printf("[%s] Ingress deleted: %s", clusterLabel, key)
				registry.remove(key)
			}
		}
	}
}

func ingressKey(clusterLabel string, ingress *networkingv1.Ingress) string {
	return clusterLabel + "/" + ingress.Namespace + "/" + ingress.Name
}

// processIngress returns the desired hostname -> IPs mapping for an ingress.
func processIngress(ingress *networkingv1.Ingress, ingressIP string) map[string][]string {
	results := make(map[string][]string)

	ips := extractIngressIPs(ingress)
	if len(ips) == 0 {
		if ingressIP == "" {
			log.Printf("Skipping ingress %s/%s: no LB IPs", ingress.Namespace, ingress.Name)
			return results
		}
		log.Printf("Ingress %s/%s has no LB IPs, using --ingress-ip %s", ingress.Namespace, ingress.Name, ingressIP)
		ips = []string{ingressIP}
	}

	for _, rule := range ingress.Spec.Rules {
		hostname := strings.TrimSpace(strings.ToLower(rule.Host))
		if !strings.HasSuffix(hostname, ".local") {
			continue
		}
		results[hostname] = unique(append(results[hostname], ips...))
	}

	return results
}

func extractIngressIPs(ingress *networkingv1.Ingress) []string {
	results := make([]string, 0)

	for _, lb := range ingress.Status.LoadBalancer.Ingress {
		if lb.IP != "" {
			results = append(results, lb.IP)
		}

		if lb.Hostname != "" {
			ips, err := net.LookupIP(lb.Hostname)
			if err != nil {
				log.Printf(
					"Failed to resolve LB hostname %s: %v",
					lb.Hostname,
					err,
				)
				continue
			}

			for _, ip := range ips {
				if ip.To4() != nil {
					results = append(results, ip.String())
				}
			}
		}
	}

	return unique(results)
}

func processManualFile(path string) (map[string][]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var entries []ManualEntry

	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, err
	}

	results := make(map[string][]string)

	for _, e := range entries {
		hostname := normalizeHostname(e.Hostname)
		if hostname == ".local" || net.ParseIP(e.IP) == nil {
			log.Printf("Skipping invalid manual entry %q -> %q", e.Hostname, e.IP)
			continue
		}
		results[hostname] = unique(append(results[hostname], e.IP))
	}

	return results, nil
}

func normalizeHostname(host string) string {
	host = strings.TrimSpace(strings.ToLower(host))

	if !strings.HasSuffix(host, ".local") {
		host = host + ".local"
	}

	return host
}

func publishMDNS(hostname string, ips []string) (*zeroconf.Server, error) {
	instance := strings.TrimSuffix(hostname, ".local")

	info := []string{
		fmt.Sprintf("ip=%s", strings.Join(ips, ",")),
		fmt.Sprintf("hostname=%s", hostname),
	}

	server, err := zeroconf.RegisterProxy(instance, "_http._tcp", "local.", 80, hostname, ips, info, nil)
	if err != nil {
		return nil, err
	}

	log.Printf("Published mDNS: %s -> %v", hostname, ips)
	return server, nil
}

func unique(input []string) []string {
	seen := map[string]bool{}
	result := make([]string, 0)

	for _, v := range input {
		if !seen[v] {
			seen[v] = true
			result = append(result, v)
		}
	}

	return result
}
