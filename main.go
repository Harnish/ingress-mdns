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
//   - If ingress has no LB IP, it is skipped
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

type PublishedService struct {
	Server *zeroconf.Server
	Name   string
}

type ServiceRegistry struct {
	mu       sync.Mutex
	services map[string][]PublishedService
}

func newServiceRegistry() *ServiceRegistry {
	return &ServiceRegistry{services: make(map[string][]PublishedService)}
}

func (r *ServiceRegistry) update(key string, svcs []PublishedService) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, svc := range r.services[key] {
		if svc.Server != nil {
			svc.Server.Shutdown()
		}
	}
	if len(svcs) > 0 {
		r.services[key] = svcs
	} else {
		delete(r.services, key)
	}
}

func (r *ServiceRegistry) remove(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, svc := range r.services[key] {
		if svc.Server != nil {
			svc.Server.Shutdown()
		}
	}
	delete(r.services, key)
}

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

func (r *ServiceRegistry) shutdownAll() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for key, svcs := range r.services {
		for _, svc := range svcs {
			if svc.Server != nil {
				svc.Server.Shutdown()
			}
		}
		delete(r.services, key)
	}
}

func main() {
	var kubeconfig string
	var manualFile string

	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to kubeconfig")
	flag.StringVar(&manualFile, "manual", "", "Path to manual JSON file")
	flag.Parse()

	if kubeconfig == "" {
		log.Fatal("--kubeconfig is required")
	}

	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		log.Fatalf("failed to load kubeconfig: %v", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalf("failed to create k8s client: %v", err)
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

	go watchIngresses(ctx, clientset, registry)

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
func watchIngresses(ctx context.Context, clientset *kubernetes.Clientset, registry *ServiceRegistry) {
	for {
		if ctx.Err() != nil {
			return
		}

		list, err := clientset.NetworkingV1().Ingresses("").List(ctx, metav1.ListOptions{})
		if err != nil {
			log.Printf("failed to list ingresses: %v; retrying in 5s", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
				continue
			}
		}

		for i := range list.Items {
			ingress := &list.Items[i]
			registry.update(ingressKey(ingress), processIngress(ingress))
		}

		log.Printf("Watching ingresses (resourceVersion %s)", list.ResourceVersion)

		watcher, err := clientset.NetworkingV1().Ingresses("").Watch(ctx, metav1.ListOptions{
			ResourceVersion: list.ResourceVersion,
		})
		if err != nil {
			log.Printf("failed to start watch: %v; retrying in 5s", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
				continue
			}
		}

		ctxDone := drainWatchEvents(ctx, watcher.ResultChan(), registry)
		watcher.Stop()

		if ctxDone {
			return
		}

		log.Println("Watch ended, re-listing...")
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

// drainWatchEvents processes events until the channel closes or ctx is done.
// Returns true if ctx was cancelled, false if the watch channel closed.
func drainWatchEvents(ctx context.Context, ch <-chan watch.Event, registry *ServiceRegistry) bool {
	for {
		select {
		case <-ctx.Done():
			return true
		case event, ok := <-ch:
			if !ok {
				return false
			}
			if event.Type == watch.Error {
				log.Printf("Watch error event: %v", event.Object)
				return false
			}
			ingress, ok := event.Object.(*networkingv1.Ingress)
			if !ok {
				continue
			}
			key := ingressKey(ingress)
			switch event.Type {
			case watch.Added, watch.Modified:
				registry.update(key, processIngress(ingress))
			case watch.Deleted:
				log.Printf("Ingress deleted: %s", key)
				registry.remove(key)
			}
		}
	}
}

func ingressKey(ingress *networkingv1.Ingress) string {
	return ingress.Namespace + "/" + ingress.Name
}

func processIngress(ingress *networkingv1.Ingress) []PublishedService {
	results := make([]PublishedService, 0)

	ips := extractIngressIPs(ingress)
	if len(ips) == 0 {
		log.Printf(
			"Skipping ingress %s/%s: no LB IPs",
			ingress.Namespace,
			ingress.Name,
		)
		return results
	}

	for _, rule := range ingress.Spec.Rules {
		if rule.Host == "" || !strings.HasSuffix(rule.Host, ".local") {
			continue
		}

		hostname := normalizeHostname(rule.Host)

		for _, ip := range ips {
			svc, err := publishMDNS(hostname, ip)
			if err != nil {
				log.Printf(
					"Failed to publish %s -> %s: %v",
					hostname,
					ip,
					err,
				)
				continue
			}

			results = append(results, svc)
		}
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

func processManualFile(path string) ([]PublishedService, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var entries []ManualEntry

	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, err
	}

	results := make([]PublishedService, 0)

	for _, entry := range entries {
		hostname := normalizeHostname(entry.Hostname)

		svc, err := publishMDNS(hostname, entry.IP)
		if err != nil {
			log.Printf(
				"Failed to publish manual entry %s -> %s: %v",
				hostname,
				entry.IP,
				err,
			)
			continue
		}

		results = append(results, svc)
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

func publishMDNS(hostname string, ip string) (PublishedService, error) {

	instance := strings.TrimSuffix(hostname, ".local")

	info := []string{
		fmt.Sprintf("ip=%s", ip),
		fmt.Sprintf("hostname=%s", hostname),
		fmt.Sprintf("published=%d", time.Now().Unix()),
	}

	server, err := zeroconf.RegisterProxy(
		instance,
		"_http._tcp",
		"local.",
		80,
		hostname,
		[]string{ip},
		info,
		nil,
	)
	if err != nil {
		return PublishedService{}, err
	}

	log.Printf("Published mDNS: %s -> %s", hostname, ip)

	return PublishedService{
		Server: server,
		Name:   hostname,
	}, nil
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
