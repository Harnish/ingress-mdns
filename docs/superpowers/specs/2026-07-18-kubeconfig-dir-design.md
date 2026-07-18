# Kubeconfig directory support (multi-cluster)

## Problem

`ingress-mdns` currently accepts a single `--kubeconfig` flag and watches
ingresses on exactly one cluster. Users running multiple clusters want one
`ingress-mdns` process to publish mDNS entries from ingresses across all of
them, driven by a directory of kubeconfig files.

## Flag behavior

- New `--kubeconfig-dir` flag.
- Mutually exclusive with `--kubeconfig`. After `flag.Parse()`, validate that
  exactly one of `--kubeconfig` / `--kubeconfig-dir` is set; fatal otherwise
  (same severity as today's "`--kubeconfig` is required" check).
- If the directory itself is missing/unreadable at startup: fatal.

## Directory polling

- A `clusterManager` tracks currently-running clusters as
  `map[path]context.CancelFunc`.
- Poll loop: every 10s (const, not a flag), `os.ReadDir(dir)`, skipping
  subdirectories and dotfiles. Do an initial poll immediately at startup
  (don't wait for the first tick).
- Diff the file list against the known-running set:
  - **New file**: `clientcmd.BuildConfigFromFlags("", path)` then
    `kubernetes.NewForConfig`. On error: log once, track the path in a
    `map[path]bool` "last failed" set so repeated failures don't re-log every
    tick, and retry on the next poll (self-heals if the file is fixed later).
    On success: create a per-cluster `context.WithCancel(ctx)`, spawn
    `go watchIngresses(clusterCtx, clientset, registry, clusterLabel)`, store
    the cancel func.
  - **Removed file**: call the stored cancel func, then
    `registry.removeCluster(clusterLabel)` to shut down and drop that
    cluster's published mDNS entries.
- `clusterLabel` = the file's base name with its extension stripped (e.g.
  `prod.yaml` -> `prod`). Used in registry keys and log lines.
- No fsnotify dependency — stdlib polling only.

## Registry / key changes

- `ingressKey` changes from `namespace/name` to `cluster/namespace/name` so
  entries from different clusters can't collide.
- `watchIngresses` and `processIngress` gain a `clusterLabel string`
  parameter, threaded into keys and log messages.
- `ServiceRegistry` gains `removeCluster(label string)`: shuts down and
  deletes every registry entry whose key is prefixed `label + "/"`.
- The manual-file entry's registry key stays `"manual"` — unaffected by
  clustering, since it isn't tied to any one cluster.

## Error handling

- Bad/unreadable kubeconfig file: skip, log once, retry each poll tick.
- Cluster API unreachable after the client is built: handled by
  `watchIngresses`'s existing 5s retry/backoff loop — no change needed.
- Missing/unreadable `--kubeconfig-dir` at startup: fatal.

## Out of scope

- Live reload is limited to add/remove of files in the directory; editing a
  kubeconfig file in place (e.g. rotating credentials) is not detected —
  users must remove+re-add the file to force a reload, or restart.
- No per-cluster flag/config for polling interval; hardcoded 10s.

## Testing

- Unit test around the diff logic: using real temp-dir files
  (`os.MkdirTemp`), simulate two poll passes (file added, then file removed),
  assert the manager starts/cancels the right clusters. No mocking
  framework — plain stdlib `os` + table-driven assertions.
