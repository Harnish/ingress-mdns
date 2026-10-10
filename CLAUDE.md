# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Does

`ingress-mdns` is a small Go daemon (`main.go`, `cluster_manager.go`) that reads Kubernetes Ingress resources from one or more clusters, extracts hostnames and load-balancer IPs, and publishes them as mDNS (Bonjour/zeroconf) A-record announcements on the local network. It also accepts a JSON file of manual hostname→IP mappings. Only `.local` hostnames are published (mDNS requirement); non-`.local` ingress rules are silently skipped.

## Commands

```bash
# Build
go build -o ingress-mdns main.go

# Run
./ingress-mdns --kubeconfig ~/.kube/config
./ingress-mdns --kubeconfig ~/.kube/config --manual ./manual.json

# Multi-cluster: one kubeconfig file per cluster (mutually exclusive with --kubeconfig)
./ingress-mdns --kubeconfig-dir /etc/ingress-mdns/kubeconfigs

# Lint
go vet ./...

# Tests
go test ./...
```

## Architecture

Code lives in `main.go` (registry, watch loop, publishing) and `cluster_manager.go` (multi-cluster). Flow:

1. Parse `--kubeconfig` or `--kubeconfig-dir` (exactly one required, `validateKubeconfigFlags`), optional `--manual`, and optional `--ingress-ip` (fallback IP for ingresses with no LB IP) flags.
2. Single cluster: build a `k8s.io/client-go` client from the kubeconfig, label `"default"`. Multi-cluster: `clusterManager` polls the directory every 10s and runs one `watchIngresses` goroutine per file, labeled by file name minus extension (`prod.yaml` → `prod`).
3. List all `NetworkingV1` Ingresses cluster-wide, extract `.local` hostnames and LB IPs via `processIngress` → `extractIngressIPs`.
4. Optionally parse the manual JSON file (`[]ManualEntry{Hostname, IP}`) via `processManualFile`.
5. For each hostname (with all its IPs), call `publishMDNS` which registers a `grandcat/zeroconf` proxy server advertising `_http._tcp` on port 80. The zeroconf server runs in the background until shutdown.
6. Block on `SIGINT`/`SIGTERM`, then call `Server.Shutdown()` on every registered entry.

Key behaviors:
- `watchIngresses` goroutine: lists all ingresses on start, then opens a Kubernetes watch from that `resourceVersion`. On disconnect or error it re-lists and re-watches automatically.
- `ServiceRegistry` (mutex-protected `map[string]*entry` keyed by `clusterLabel/namespace/name`) tracks live zeroconf servers. `update` takes a desired hostname→IPs map, is a no-op if unchanged, else shuts down old servers and starts one server per hostname (all IPs). `remove` cleans up on delete; `prune(label, keep)` drops that cluster's keys missing after a re-list; `removeCluster(label)` drops all of a cluster's keys.
- Manual entries are stored under the `"manual"` key, validated (non-empty host, valid IP), and never pruned (prune and `removeCluster` only touch `label/` prefixes).
- `clusterManager`: new kubeconfig file → start watcher; file removed → cancel its context, and the goroutine calls `removeCluster`. Files starting with `.` and directories are ignored; load failures are logged once per file. Not goroutine-safe by design — only `run()` touches its maps.
- `extractIngressIPs` resolves LB hostnames to IPv4 at the time the ingress is processed; IP changes only take effect when the ingress is next modified or the watch reconnects.
