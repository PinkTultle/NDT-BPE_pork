#!/usr/bin/env bash
set -euo pipefail

# 스크립트가 위치한 최상위 디렉토리 절대 경로 저장
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

git submodule sync --recursive

if [[ -d storage/spdk && ! -f storage/spdk/.git && ! -d storage/spdk/.git ]]; then
  # 일반 폴더가 이미 있으면 안내 후 종료 (자동 삭제는 위험)
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

# 1. SPDK 빌드
echo "========================================================"
echo "[INFO] Building SPDK..."
echo "========================================================"
cd "${ROOT_DIR}/storage/spdk"
sudo ./scripts/pkgdep.sh
sudo ./configure --disable-unit-tests
sudo make -j$(nproc)

# 2. Compute 빌드
echo "========================================================"
echo "[INFO] Moving to compute directory to run build scripts..."
echo "========================================================"
cd "${ROOT_DIR}/compute"

# 주의: 이전에 수정한 build_all.sh 내부에 가상환경(venv) 체크 로직이 있으므로,
# 이 전체 스크립트를 실행하기 전에 반드시 venv를 activate 해두어야 합니다.
./bootstrap.sh
./build_all.sh

echo "========================================================"
echo "🎉 [OK] All tasks completed successfully!"
echo "========================================================"
