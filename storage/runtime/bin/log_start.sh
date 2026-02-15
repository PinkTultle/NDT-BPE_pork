#!/bin/bash
set -euo pipefail

BIN="./bpe_process"          # 실행 바이너리
CORE=5                       # pinning 할 코어 번호
MP_INT=0.5                   # mpstat 샘플링 간격(초)
CPU_LOG="cpu_core${CORE}.log"
APP_LOG="bpe_process.log"

# mpstat 백그라운드로 코어 5 사용률 로깅 시작
# sysstat 패키지가 설치되어 있어야 합니다 (Ubuntu: sudo apt-get install -y sysstat)
mpstat -P ${CORE} ${MP_INT} > "${CPU_LOG}" &
MPSTAT_PID=$!

# bpe_process를 코어 5에 고정해서 실행(+ 실행 시간/리소스 로그)
echo "=== [$(date)] start bpe_process on CPU ${CORE} ===" | tee -a "${APP_LOG}"
/usr/bin/time -f "elapsed=%E user=%U sys=%S maxrss_kb=%M" \
  taskset -c ${CORE} ${BIN} 2>&1 | tee -a "${APP_LOG}"
RET=$?

# mpstat 종료
kill ${MPSTAT_PID} 2>/dev/null || true
wait ${MPSTAT_PID} 2>/dev/null || true

echo "=== [$(date)] done (rc=${RET}) ===" | tee -a "${APP_LOG}"
exit ${RET}

