#!/bin/sh
set -e

systemctl daemon-reload >/dev/null 2>&1 || true

cat <<'EOF'
ingress-mdns installed.

Next steps:
  1. Place a kubeconfig at /etc/ingress-mdns/kubeconfig
  2. (Optional) edit /etc/ingress-mdns/manual.json
  3. Adjust args in /etc/ingress-mdns/ingress-mdns.env
  4. Enable + start:
       systemctl enable --now ingress-mdns
EOF

exit 0
