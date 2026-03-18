#!/usr/bin/env bash
set -euo pipefail

# 스크립트가 위치한 최상위 디렉토리 절대 경로 저장
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

git submodule sync --recursive

if [[ -d storage/spdk && ! -f storage/spdk/.git && ! -d storage/spdk/.git ]]; then
  echo "[ERR] storage/spdk exists but is not a submodule checkout."
  echo "Move or remove it, then re-run:"
  echo "  mv storage/spdk storage/spdk.backup.\$(date +%s)"
  exit 1
fi

git submodule update --init --recursive

PIN="ec7092bcaa5c7f19574b41c8d7c8b44ab1b2927c"
CUR="$(git -C storage/spdk rev-parse HEAD)"
[[ "$CUR" == "$PIN" ]] || { echo "[ERR] spdk=$CUR expected=$PIN" >&2; exit 1; }

echo "[OK] spdk pinned to $PIN"

# 1. SPDK 세팅 및 빌드
echo "========================================================"
echo "[INFO] Building SPDK..."
echo "========================================================"
cd "${ROOT_DIR}/storage/spdk"
sudo ./scripts/pkgdep.sh
sudo ./configure --disable-unit-tests
sudo make -j$(nproc)

# 2. Compute 빌드 및 venv 활성화
echo "========================================================"
echo "[INFO] Moving to compute directory and activating venv..."
echo "========================================================"
cd "${ROOT_DIR}/compute"

# venv 폴더가 없으면 자동으로 생성
if [[ ! -d "venv" ]]; then
    echo "[INFO] Creating Python virtual environment (venv)..."
    python3 -m venv venv
fi

# 가상환경 활성화
source venv/bin/activate
echo "[INFO] venv activated: $VIRTUAL_ENV"

# 가상환경이 켜진 상태에서 하위 빌드 스크립트 실행
./bootstrap.sh
./build_all.sh

echo "========================================================"
echo "[OK] All tasks completed successfully!"
echo "========================================================"
