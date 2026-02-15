#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OVERLAY="$ROOT/update/spdk"
SPDK="$ROOT/spdk"

need_dir() { test -d "$1" || { echo "Missing dir: $1"; exit 1; }; }
need_dir "$OVERLAY"
need_dir "$SPDK"

# Ensure submodule is initialized
git -C "$SPDK" rev-parse --is-inside-work-tree >/dev/null 2>&1 || {
  echo "SPDK dir is not a git repo. Did you init submodule?"
  exit 1
}

echo "SPDK (target):   $SPDK"
echo "Overlay (source):$OVERLAY"
echo

echo "[1/3] Preview changes (dry-run)"
rsync -avun --itemize-changes \
  --exclude='.git/' \
  "$OVERLAY"/ "$SPDK"/

echo
echo "[2/3] Apply overlay -> SPDK"
rsync -avu --itemize-changes \
  --exclude='.git/' \
  "$OVERLAY"/ "$SPDK"/

echo
echo "[3/3] SPDK working tree status:"
git -C "$SPDK" status --porcelain || true
