#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# [0] 사전 검증: Python 가상환경(venv) 실행 여부 확인
# ==============================================================================
if [[ -z "${VIRTUAL_ENV:-}" ]]; then
  echo "❌ [ERROR] 가상환경(venv)이 활성화되지 않았습니다." >&2
  echo "💡 실행 방법: 먼저 'source ../scripts/.venv/bin/activate' (또는 해당 venv 경로)를 실행한 뒤 다시 시도해주세요." >&2
  exit 1
fi

echo "✅ [0/6] 가상환경(venv) 확인 완료: $VIRTUAL_ENV"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
JOBS="${JOBS:-$(nproc)}"

LIBURING_DIR="${SCRIPT_DIR}/third_party/liburing"
TOKENIZERS_CPP_DIR="${SCRIPT_DIR}/third_party/tokenizers-cpp"
TOKENIZERS_CPP_BUILD_DIR="${TOKENIZERS_CPP_DIR}/build"

# ==============================================================================
# [1] 서브모듈 초기화
# ==============================================================================
echo "🔄 [1/6] 서브모듈 동기화 및 초기화 진행 중..."
git -C "${REPO_ROOT}" submodule update --init --recursive \
  compute/third_party/liburing \
  compute/third_party/tokenizers-cpp

# ==============================================================================
# [2] liburing 라이브러리 빌드 (io_uring 지원)
# ==============================================================================
echo "🔨 [2/6] liburing 빌드 확인 중..."
if [[ ! -f "${LIBURING_DIR}/src/liburing.a" ]]; then
  echo "  -> liburing 새로 빌드 중..."
  (
    cd "${LIBURING_DIR}"
    ./configure
    make -j"${JOBS}"
  )
else
  echo "  -> liburing 이미 빌드되어 있습니다. (Skip)"
fi

# ==============================================================================
# [3] tokenizers-cpp 라이브러리 빌드
# ==============================================================================
echo "🔨 [3/6] tokenizers-cpp 빌드 확인 중..."
if [[ ! -f "${TOKENIZERS_CPP_BUILD_DIR}/libtokenizers_cpp.a" ]]; then
  echo "  -> tokenizers-cpp 새로 빌드 중..."
  cmake -S "${TOKENIZERS_CPP_DIR}" -B "${TOKENIZERS_CPP_BUILD_DIR}" -DCMAKE_BUILD_TYPE=Release
  cmake --build "${TOKENIZERS_CPP_BUILD_DIR}" -j "${JOBS}"
else
  echo "  -> tokenizers-cpp 이미 빌드되어 있습니다. (Skip)"
fi

# ==============================================================================
# [4] Compute C++ 코어 라이브러리 빌드
# ==============================================================================
echo "🔨 [4/6] Compute 코어 라이브러리(C++) 빌드 중..."
make -C "${SCRIPT_DIR}" -j"${JOBS}"

# ==============================================================================
# [5] Python 종속성(pyarrow) 설치 및 경로 탐색
# ==============================================================================
echo "🐍 [5/6] Python API 빌드를 위한 pyarrow 확인 및 경로 탐색 중..."
# pyarrow가 없으면 설치 (setup.py의 하드코딩 에러 방지용)
pip3 install pyarrow --break-system-packages

# 설치된 pyarrow의 실제 경로를 찾아 환경 변수로 등록
export PYARROW_ROOT=$(python3 -c "import pyarrow, os; print(os.path.dirname(pyarrow.__file__))")
echo "  -> 찾은 pyarrow 경로: $PYARROW_ROOT"

# ==============================================================================
# [6] Python API 바인딩 (pybind11) 설치
# ==============================================================================
echo "📦 [6/6] Python API (ndt_compute) 모듈 설치 중..."
# venv 내부이므로 --break-system-packages 없이 안전하게 설치 가능
pip3 install -e "${SCRIPT_DIR}" --break-system-packages

echo "=============================================================================="
echo "🎉 [완료] 모든 빌드 및 설치가 성공적으로 끝났습니다!"
echo "  - Tokenizer libs : ${TOKENIZERS_CPP_BUILD_DIR}"
echo "  - Compute archive: ${SCRIPT_DIR}/build/lib/libndt_compute.a"
echo "  - Python module  : 'import ndt_compute' 로 사용 가능합니다."
echo "=============================================================================="
