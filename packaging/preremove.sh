#!/bin/sh
set -e

if [ -d /run/systemd/system ]; then
    systemctl stop ingress-mdns >/dev/null 2>&1 || true
    systemctl disable ingress-mdns >/dev/null 2>&1 || true
fi

exit 0
