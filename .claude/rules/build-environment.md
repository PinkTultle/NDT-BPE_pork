---
description: "임베디드 빌드 환경 — 크로스 컴파일, 빌드 산출물, .gitignore"
paths:
  - "Makefile"
  - "**/Makefile"
  - "CMakeLists.txt"
  - "**/CMakeLists.txt"
  - "**/*.mk"
  - "**/*.cmake"
  - "**/*.ld"
---

# 임베디드 빌드 환경

> 원본: `blueprints/build-environment.md`
> 프로젝트 rules 템플릿 — `/adt:project-configure`가 프로젝트에 맞게 커스터마이즈하여 복제

## 1. 크로스 컴파일 기본 원칙

- 호스트/타겟 툴체인 명확히 구분
- `which arm-linux-gnueabihf-gcc` 등으로 툴체인 존재 먼저 확인
- Makefile 변수:

```makefile
CROSS_COMPILE ?= arm-linux-gnueabihf-
CC  = $(CROSS_COMPILE)gcc
CXX = $(CROSS_COMPILE)g++
AR  = $(CROSS_COMPILE)ar
```

## 2. 빌드 산출물 관리

```
project/
├── src/          # 소스
├── include/      # 헤더
├── build/        # 빌드 산출물 (git 제외)
│   ├── debug/
│   └── release/
├── docs/         # 구현 문서
└── scripts/      # 자동화 스크립트
```

## 3. .gitignore 필수 항목 (C/C++ 임베디드)

```gitignore
# 빌드 산출물
build/
*.o
*.a
*.so
*.elf
*.bin
*.hex
*.map

# IDE
.vscode/settings.json
*.launch
*.cproject

# 디버그
*.log
core
```
