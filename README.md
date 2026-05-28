# ingress-mdns

A small Go daemon for home lab use. It watches Kubernetes Ingress resources and advertises their hostnames as mDNS (Bonjour/zeroconf) A records on the local network — so devices on the LAN can resolve `myapp.local` without a dedicated DNS server.

This exists because my home lab runs k3s with most services exposed only internally via `.local` hostnames. Rather than maintaining a separate DNS server or editing `/etc/hosts` on every device, `ingress-mdns` reads the cluster's Ingress objects and publishes mDNS announcements automatically.

## How it works

1. Connects to the cluster using a kubeconfig file.
2. Lists all Ingress resources cluster-wide and watches for changes.
3. For each Ingress rule with a `.local` hostname and a load-balancer IP, publishes an mDNS host announcement via `grandcat/zeroconf`.
4. Reconciles automatically — adding, updating, and removing announcements as Ingresses change.
5. Optionally reads a JSON file of manual hostname→IP mappings for non-Ingress services (printers, NAS, etc.).

Only `.local` hostnames are published; non-`.local` rules are silently skipped (a hard requirement of mDNS).

## Requirements

- Go 1.24+
- A kubeconfig with read access to Ingress resources
- The machine running `ingress-mdns` must be on the same LAN segment as the devices that need resolution

## Build

```bash
go build -o ingress-mdns main.go
```

## Usage

```bash
# Basic — watch ingresses only
./ingress-mdns --kubeconfig ~/.kube/config

# With manual entries for non-Ingress hosts
./ingress-mdns --kubeconfig ~/.kube/config --manual ./manual.json
```

### Flags

| Flag | Required | Description |
|------|----------|-------------|
| `--kubeconfig` | Yes | Path to kubeconfig file |
| `--manual` | No | Path to JSON file with manual hostname→IP mappings |

### Manual entries file

Useful for advertising hosts that aren't Kubernetes Ingresses — a NAS, a printer, a Pi running something outside the cluster, etc.

```json
[
  {
    "hostname": "nas.local",
    "ip": "192.168.1.20"
  },
  {
    "hostname": "printer.local",
    "ip": "192.168.1.50"
  }
]
```

Manual entries are loaded once at startup and are not reconciled by the watch loop. Restart the daemon to pick up changes.

## Running as a systemd service

```ini
[Unit]
Description=Kubernetes Ingress mDNS advertiser
After=network-online.target
Wants=network-online.target

[Service]
ExecStart=/usr/local/bin/ingress-mdns --kubeconfig /etc/ingress-mdns/kubeconfig --manual /etc/ingress-mdns/manual.json
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

## Notes

- If an Ingress has no load-balancer IP yet (e.g. pending), it is skipped. It will be picked up once the watch sees it updated.
- If the LB address is a hostname rather than an IP, `ingress-mdns` resolves it to IPv4 at processing time. IP changes only take effect on the next watch event or reconnect.
- The watch connection auto-reconnects on error or disconnect — it re-lists all Ingresses first so no entries are missed.
- mDNS is link-local only. This will not work across routed network segments.
