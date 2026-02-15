#!/usr/bin/env bash
set -euo pipefail

# ---- Config (TARGET = nude1) ----
TARGET_IP="192.168.50.114"
TARGET_PORT="4420"
NQN="nqn.2025-01.io.spdk:cnode1"
MNT="/mnt/nvme"
DEV="/dev/nvme0n1"  # [변경] 디바이스 명시적 고정

echo "[INFO] Unmount ${MNT} if mounted..."
if mountPOINT=$(mount | grep "${MNT}"); then
    sudo umount "${MNT}"
fi

echo "[INFO] Disconnect all NVMe-oF..."
sudo nvme disconnect-all || true

echo "[INFO] Discover on ${TARGET_IP}:${TARGET_PORT} ..."
sudo nvme discover -t tcp -a "${TARGET_IP}" -s "${TARGET_PORT}"

echo "[INFO] Connect NQN=${NQN} ..."
sudo nvme connect -t tcp -a "${TARGET_IP}" -s "${TARGET_PORT}" -n "${NQN}"

# [추가] 커널이 장치 노드(/dev/nvme0n1)를 생성할 때까지 대기 (Race Condition 방지)
echo "[INFO] Waiting for device ${DEV} to appear..."
sleep 2

# 장치가 실제로 생겼는지 확인
if [ ! -b "${DEV}" ]; then
    echo "[FATAL] Device ${DEV} not found after connection!"
    exit 1
fi

echo "[INFO] Using device: ${DEV}"
sudo mkdir -p "${MNT}"

# 파일시스템 확인 후 없으면 포맷 (ext4)
FSTYPE="$(lsblk -no FSTYPE "${DEV}" | head -n1 || true)"
if [ -z "${FSTYPE}" ]; then
  echo "[WARN] No filesystem on ${DEV}. Formatting ext4..."
  sudo mkfs.ext4 -F "${DEV}"
fi

echo "[INFO] Mount ${DEV} -> ${MNT}"
sudo mount "${DEV}" "${MNT}"

# 권한 조정 (현재 사용자에게 쓰기 권한 부여)
echo "[INFO] Chown ${MNT} to user..."
sudo chown "$(whoami):$(whoami)" "${MNT}"

if [ -f "./wiki_corpus.txt" ]; then
  echo "[INFO] Copying corpus to remote storage..."
  cp -f ./wiki_corpus.txt "${MNT}/"
fi

echo "[SUCCESS] Connected + mounted: ${DEV} -> ${MNT}"
