---
name: project-configure
description: 대화형으로 프로젝트의 AI 도구 설정을 구체화 — 기술 스택, 모듈 구조, 컨벤션 등을 채워넣음
argument-hint: "[target-directory-path]"
allowed-tools: Read, Bash, Glob, Grep, Write, Edit
---

# Project Configure

`/project-init`으로 복제된 템플릿을 사용자와 대화하며 프로젝트에 맞게 구체화한다.

## 인자

- `$0` — 타겟 디렉토리 경로 (선택, 미지정 시 현재 디렉토리)

## 매뉴얼 저장소(blueprints) 참조

```
# project-init에 의해 절대 경로로 치환됨
REPO_DIR=/home/innn/My_AI_manual
```

이 경로의 `blueprints/` 디렉토리에서 참고 자료를 검색하여 프로젝트에 적합한 내용을 제안한다.
경로가 유효하지 않으면 사용자에게 매뉴얼 저장소 위치를 확인한다.

## 수집할 정보

프로젝트에 대해 아래 정보를 대화형으로 수집한다:

### 1. 프로젝트 기본 정보
- 프로젝트명
- 프로젝트 설명/목적
- 주요 언어/프레임워크 (C, C++, Python, Rust 등)

### 2. 임베디드 프로젝트인 경우
- 타겟 보드/MCU
- 툴체인
- OS/RTOS
- 빌드 시스템 (Make, CMake 등)

### 3. 프로젝트 구조
- 디렉토리 레이아웃 (기존 구조 감지 또는 새로 설계)
- 레이어 아키텍처 (있는 경우)
- 모듈 접두사 네이밍

### 4. 워크플로우
- 브랜치 전략
- 빌드/테스트 명령어
- Git hooks 필요 여부

### 5. AI 도구 추가 설정
- 프로젝트 레벨 스킬 필요 여부
- 특별한 코드 규칙이나 제약사항

## 실행 흐름

1. **현재 상태 파악** — 타겟 디렉토리의 `CLAUDE.md`, `.claude/` 존재 확인
   - init이 안 되어 있으면 → 먼저 `/project-init` 실행을 안내
2. **기존 프로젝트 분석** — 타겟에 이미 코드가 있으면 구조를 분석하여 제안에 활용
3. **대화형 수집** — 위 항목들을 순서대로 질문, 각 단계에서 제안과 추천 포함
4. **구성 플랜 작성** — 수집된 정보로 구성 계획서를 작성하여 사용자 검토
5. **적용** — 승인 후 `CLAUDE.md` 및 관련 파일의 placeholder를 실제 값으로 채움
6. **결과 요약** — 변경된 파일 목록과 프로젝트 설정 요약 출력

## blueprints 참조 규칙

수집된 기술 스택에 따라 이 저장소의 blueprints에서 적절한 내용을 참조한다:

| 조건 | 참조할 blueprint |
|------|-----------------|
| C/C++ 프로젝트 | `blueprints/coding-standards.md` |
| 임베디드 프로젝트 | `blueprints/build-environment.md` |
| Git 사용 | `blueprints/git-workflow.md` |
| 모든 프로젝트 | `blueprints/base-directives.md`, `blueprints/design-principles.md` |

## 주의사항

- 대화는 한 번에 너무 많은 질문을 하지 않는다 — 카테고리별로 나눠서 진행
- 각 단계에서 기본값/추천값을 제시하여 사용자가 빠르게 결정할 수 있도록 한다
- 구성 플랜을 반드시 사용자에게 보여주고 승인 후 적용한다
- 적용 후 자동 커밋하지 않음 — 사용자가 검토 후 커밋
