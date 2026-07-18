# ingress-mdns

A small Go daemon for home lab use. It watches Kubernetes Ingress resources and advertises their hostnames as mDNS (Bonjour/zeroconf) A records on the local network — so devices on the LAN can resolve `myapp.local` without a dedicated DNS server.

This exists because my home lab runs k3s with most services exposed only internally via `.local` hostnames. Rather than maintaining a separate DNS server or editing `/etc/hosts` on every device, `ingress-mdns` reads the cluster's Ingress objects and publishes mDNS announcements automatically.

## How it works

1. Connects to one cluster via `--kubeconfig`, or to several via `--kubeconfig-dir` (one file per cluster).
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

# Multiple clusters — one kubeconfig file per cluster in a directory
./ingress-mdns --kubeconfig-dir /etc/ingress-mdns/kubeconfigs
```

### Flags

| Flag | Required | Description |
|------|----------|-------------|
| `--kubeconfig` | One of `--kubeconfig` / `--kubeconfig-dir` | Path to a single kubeconfig file |
| `--kubeconfig-dir` | One of `--kubeconfig` / `--kubeconfig-dir` | Directory of kubeconfig files, one per cluster |
| `--manual` | No | Path to JSON file with manual hostname→IP mappings |

`--kubeconfig` and `--kubeconfig-dir` are mutually exclusive — pick one.

With `--kubeconfig-dir`, the directory is polled every 10s: adding a kubeconfig file starts watching that cluster, removing one stops it and tears down its published mDNS entries. The cluster's name (for logs and internal keys) is its filename with the extension stripped, e.g. `prod.yaml` → `prod`. A kubeconfig that fails to parse is logged once and retried on the next poll — it doesn't stop the daemon or the other clusters.

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

## Install (deb / rpm)

Pre-built `.deb` and `.rpm` packages are published on the
[Releases](https://github.com/Harnish/ingress-mdns/releases) page for `amd64`
and `arm64`. They install:

- the binary at `/usr/bin/ingress-mdns`
- a systemd unit at `/lib/systemd/system/ingress-mdns.service`
- config under `/etc/ingress-mdns/`

```bash
# Debian / Ubuntu
sudo dpkg -i ingress-mdns_*_amd64.deb

# RHEL / Fedora
sudo rpm -i ingress-mdns-*.x86_64.rpm
```

After installing:

1. Place a kubeconfig at `/etc/ingress-mdns/kubeconfig`.
2. (Optional) create `/etc/ingress-mdns/manual.json` for non-Ingress hosts.
3. Adjust arguments in `/etc/ingress-mdns/ingress-mdns.env`.
4. Enable and start:

```bash
sudo systemctl enable --now ingress-mdns
```

### Building packages locally

```bash
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o dist/ingress-mdns main.go
ARCH=amd64 VERSION=0.0.1 nfpm package --config nfpm.yaml --packager deb --target dist/
ARCH=amd64 VERSION=0.0.1 nfpm package --config nfpm.yaml --packager rpm --target dist/
```

The [`release` workflow](.github/workflows/release.yml) builds both formats for
`amd64` and `arm64` automatically and attaches them to a GitHub Release when a
`v*` tag is pushed.

## Running as a systemd service (manual)

The packaged unit lives at [`packaging/ingress-mdns.service`](packaging/ingress-mdns.service).
It reads arguments from `/etc/ingress-mdns/ingress-mdns.env`:

```ini
[Unit]
Description=Kubernetes Ingress mDNS advertiser
After=network-online.target
Wants=network-online.target

[Service]
EnvironmentFile=-/etc/ingress-mdns/ingress-mdns.env
ExecStart=/usr/bin/ingress-mdns $INGRESS_MDNS_ARGS
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
