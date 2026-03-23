# NDT-BPE — Claude Code 프로젝트 규칙
> 위치: `<프로젝트 루트>/CLAUDE.md`
> 글로벌 규칙(`~/.claude/CLAUDE.md`)을 상속하며, 이 파일에서 오버라이드/추가합니다.

---

## 프로젝트 개요

```
프로젝트명  : NDT-BPE (Near-Data Tokenization for BPE)
설명        : NVMe-oF 기반 disaggregated storage에서 커널을 우회(bypass)하여
              BPE 토큰화를 스토리지 사이드에서 직접 처리, IO 병목을 줄이는
              Near-Data Processing 연구 프로젝트 (학습용 fork)
주요 언어   : C++17, Python, Bash
```

---

## 1. 프로젝트 디렉토리 구조

```
NDT-BPE/
├── CLAUDE.md
├── bootstrap.sh              ← 전체 빌드 (SPDK + compute)
├── compute/                  ← Host(compute) 측 코드
│   ├── src/                  ← C++ 소스 (io_uring, FIEMAP, Arrow, pybind11)
│   ├── include/              ← 헤더 파일
│   ├── third_party/          ← liburing, tokenizers-cpp (서브모듈)
│   ├── Makefile
│   ├── build_all.sh          ← compute 빌드 스크립트
│   └── setup.py              ← Python 바인딩 (ndt_compute)
├── storage/                  ← Storage 측 코드
│   ├── runtime/              ← BPE 토큰화 런타임 (SysV IPC 기반)
│   │   ├── src/              ← main, bpe_tokenizer, common
│   │   ├── include/
│   │   └── Makefile
│   ├── spdk/                 ← SPDK NVMe-oF 타겟 (서브모듈, pinned)
│   ├── update/               ← SPDK 오버레이 패치
│   ├── setting.sh            ← 커널 모듈 로드 (nvme_tcp, nvme_fabrics)
│   └── start.sh              ← NVMe-oF discover → connect → mount
├── scripts/                  ← 실험 스크립트 (Python)
│   ├── 기본데이터/            ← 기본 실험 (baseline vs NDP)
│   └── 멀티테넌트/            ← 멀티테넌트 실험
└── test_parser.py
```

---

## 2. 환경별 설정

### WSL2
```bash
# WSL2에서는 코드 분석/리뷰만 수행 (SPDK/NVMe 하드웨어 없음)
# 빌드 테스트: compute/ 영역만 가능 (liburing는 빌드되나 실행 불가할 수 있음)
```

### SSH (실제 스토리지 서버)
```bash
# NVMe-oF 연결 전 커널 모듈 로드 필요
sudo modprobe nvme_tcp
sudo modprobe nvme_fabrics

# 환경 변수는 .env 파일로 관리 (STORAGE_IP, TARGET_PORT, NQN, MNT, DEV)
```

---

## 3. 아키텍처 규칙

```
┌─────────────────────────────────────────────────────────┐
│  Host (compute/)                                        │
│  ┌──────────┐  ┌────────────┐  ┌──────────────────┐    │
│  │ FIEMAP   │→│ LBA 변환   │→│ NVMe passthrough  │    │
│  │ resolver │  │ (extent)   │  │ (io_uring, 0xD4) │    │
│  └──────────┘  └────────────┘  └────────┬─────────┘    │
│                                          │ NVMe-oF      │
├──────────────────────────────────────────┼──────────────┤
│  Storage Server                          ▼              │
│  ┌──────────────┐  ┌─────────────────────────────┐     │
│  │ SPDK Target  │←→│ runtime/ (BPE tokenizer)    │     │
│  │ (NVMe-oF)    │  │ SysV SHM + MsgQueue 기반    │     │
│  └──────────────┘  └─────────────────────────────┘     │
└─────────────────────────────────────────────────────────┘
핵심: 데이터가 Host로 돌아오지 않고 Storage 측에서 in-place 처리
```

### 주요 데이터 흐름
1. **TXT 경로**: Host가 FIEMAP으로 파일의 물리 위치(LBA) 계산 → NVMe passthrough(0xD4)로 Storage에 전달 → Storage runtime이 해당 LBA에서 직접 읽어 토큰화
2. **Arrow 경로**: Host가 Arrow 메타데이터에서 텍스트 컬럼의 바이트 범위 → LBA extent-index 생성 → Storage가 LBA만으로 처리 (Arrow 파서 불필요)

### 핵심 기술 스택
| 기술 | 역할 | 위치 |
|------|------|------|
| **io_uring** | 비동기 NVMe passthrough 커맨드 제출 | `compute/src/io-uring.cpp` |
| **FIEMAP** | 파일 → 물리 LBA 매핑 (커널 ioctl) | `compute/src/fiemap_schedule.cpp` |
| **SPDK** | 유저스페이스 NVMe-oF 타겟 (커널 bypass) | `storage/spdk/` |
| **SysV IPC** | 공유 메모리 + 메시지 큐 (SPDK ↔ runtime) | `storage/runtime/src/common.cpp` |
| **tokenizers-cpp** | BPE 토큰화 엔진 | `storage/runtime/src/bpe_tokenizer.cpp` |
| **pybind11** | C++ → Python 바인딩 | `compute/src/bindings.cpp` |
| **Apache Arrow** | 구조화 데이터셋 포맷 (IPC) | `compute/src/arrow_text_dump*.cpp` |

---

## 4. 프로젝트별 코드 규칙

### 4.1 네이밍 컨벤션
- 함수/변수: `lower_snake_case`
- 매크로/상수: `ALL_CAPS_SNAKE`
- 구조체/타입: 혼용 상태 (원본 프로젝트 컨벤션 유지)

### 4.2 프로젝트 고유 규칙
- **학습용 fork**: 코드 설명 요청 시 **설계 의도(왜 이렇게 했는지)**와 **기술 배경**을 함께 설명
- **모던 C++ 문법** (`std::move`, `std::unique_ptr`, `lambda`, `auto` 등) 사용 시 부연 설명 포함
- **커널/드라이버 관련 개념** (ioctl, FIEMAP, NVMe opcode 등)은 배경 설명 추가
- 서브모듈: SPDK는 특정 커밋에 pinned (`ec7092b...`), 임의 업데이트 금지
- `.ai-agent/` 디렉토리에 아키텍처 문서(JSON)가 존재 — 코드 분석 시 참조 가능

---

## 5. 빌드/실행 명령어

```bash
# === 전체 빌드 (SPDK + compute + Python 바인딩) ===
./bootstrap.sh

# === compute만 빌드 ===
cd compute
source venv/bin/activate
./build_all.sh          # 서브모듈 + liburing + tokenizers-cpp + C++ + Python 바인딩

# === compute C++ 코어만 (Make) ===
cd compute && make -j$(nproc)

# === storage runtime만 빌드 ===
cd storage/runtime && make

# === NVMe-oF 연결 및 마운트 (실제 서버 환경) ===
cd storage
sudo bash setting.sh    # 커널 모듈 로드
bash start.sh           # discover → connect → mount
```

---

## 6. Git 워크플로우

### 브랜치 구조
```
main              ← 현재 유일한 브랜치 (학습용 fork)
feature/xxx       ← 기능 추가/실험 시 분기
study/xxx         ← 코드 분석/학습 노트용 브랜치 (필요 시)
```

### 서브모듈 관리
```bash
# 서브모듈 초기화 (최초 clone 후)
git submodule update --init --recursive

# SPDK는 pinned commit — 업데이트하지 않음
# compute/third_party/* 는 build_all.sh가 자동 처리
```

---

## 7. 구현 문서 산출 위치

```
docs/impl/
├── YYYYMMDD_기능명.md     # 구현 완료 후 자동 산출
└── README.md              # 문서 인덱스
```

문서 형식은 글로벌 규칙 **3.3절** 형식을 따릅니다.

---

## 8. 알려진 이슈 및 해결책

| 환경 | 증상 | 해결책 |
|------|------|--------|
| WSL2 | SPDK 빌드/실행 불가 (hugepages, NVMe 디바이스 없음) | WSL2에서는 코드 분석만, 실행은 실서버에서 |
| compute | pyarrow 경로 하드코딩 (`scripts/.venv/...`) | `build_all.sh`가 `PYARROW_ROOT` 자동 탐지 |
| storage/runtime | SHM 키 충돌 가능 (`ipcrm -M 5678/5679`) | `make clean_shm` 으로 정리 |

---

*글로벌 규칙과 충돌 시 이 파일의 규칙이 우선합니다.*
