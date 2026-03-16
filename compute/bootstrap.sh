#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
JOBS="${JOBS:-$(nproc)}"
LIBURING_DIR="${SCRIPT_DIR}/third_party/liburing"
TOKENIZERS_CPP_DIR="${SCRIPT_DIR}/third_party/tokenizers-cpp"
TOKENIZERS_CPP_BUILD_DIR="${TOKENIZERS_CPP_DIR}/build"

echo "[1/5] init submodules"
git -C "${REPO_ROOT}" submodule update --init --recursive \
  compute/third_party/liburing \
  compute/third_party/tokenizers-cpp

echo "[2/5] build liburing"
if [[ ! -f "${LIBURING_DIR}/src/liburing.a" ]]; then
  (
    cd "${LIBURING_DIR}"
    ./configure
    make -j"${JOBS}"
  )
else
  echo "  - liburing already built"
fi

echo "[3/5] build tokenizers-cpp"
if [[ ! -f "${TOKENIZERS_CPP_BUILD_DIR}/libtokenizers_cpp.a" ]]; then
  cmake -S "${TOKENIZERS_CPP_DIR}" -B "${TOKENIZERS_CPP_BUILD_DIR}" -DCMAKE_BUILD_TYPE=Release
  cmake --build "${TOKENIZERS_CPP_BUILD_DIR}" -j "${JOBS}"
else
  echo "  - tokenizers-cpp already built"
fi

echo "[4/5] build compute library"
make -C "${SCRIPT_DIR}" -j"${JOBS}"

echo "[5/5] done"
echo "tokenizer libs: ${TOKENIZERS_CPP_BUILD_DIR}"
echo "compute archive: ${SCRIPT_DIR}/build/lib/libndt_compute.a"
