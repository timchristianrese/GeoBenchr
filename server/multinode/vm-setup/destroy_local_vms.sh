#!/usr/bin/env bash
# Tears down the 5 local VMs (and the libvirt network) created by setup_local_vms.sh.
# Usage: ./destroy_local_vms.sh [--keep-network] [--keep-image-cache]

set -euo pipefail

NET_NAME="${NET_NAME:-geobenchr-net}"
POOL_DIR="${POOL_DIR:-/var/lib/libvirt/images/geobenchr}"
NODES=(geobenchr-sut-1 geobenchr-sut-2 geobenchr-sut-3 geobenchr-sut-4 geobenchr-sut-5)

KEEP_NETWORK=0
KEEP_CACHE=0
for arg in "$@"; do
  case "$arg" in
    --keep-network) KEEP_NETWORK=1 ;;
    --keep-image-cache) KEEP_CACHE=1 ;;
    -h|--help) grep '^#' "$0" | sed 's/^#//'; exit 0 ;;
    *) echo "Unknown argument: $arg" >&2; exit 1 ;;
  esac
done

for node in "${NODES[@]}"; do
  if sudo virsh dominfo "$node" >/dev/null 2>&1; then
    echo ">>> Removing $node"
    sudo virsh destroy "$node" >/dev/null 2>&1 || true
    sudo virsh undefine "$node" --remove-all-storage >/dev/null 2>&1 || true
  fi
done

if [ "$KEEP_NETWORK" -eq 0 ] && sudo virsh net-info "$NET_NAME" >/dev/null 2>&1; then
  echo ">>> Removing libvirt network $NET_NAME"
  sudo virsh net-destroy "$NET_NAME" >/dev/null 2>&1 || true
  sudo virsh net-undefine "$NET_NAME" >/dev/null 2>&1 || true
fi

sudo rm -rf "$POOL_DIR"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ "$KEEP_CACHE" -eq 0 ]; then
  rm -rf "$SCRIPT_DIR/.work"
else
  # still clean up the per-node seed/user-data, just keep the downloaded base image
  find "$SCRIPT_DIR/.work" -mindepth 1 -maxdepth 1 -type d -exec rm -rf {} +
fi

echo "Done."
