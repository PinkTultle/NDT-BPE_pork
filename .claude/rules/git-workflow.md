---
description: "Git 워크플로우 — 브랜치 전략, Conventional Commits, Worktree"
# paths 없음 → 항상 로드
---

# Git 워크플로우

> 원본: `blueprints/git-workflow.md`
> 프로젝트 rules 템플릿 — `/adt:project-configure`가 프로젝트에 맞게 커스터마이즈하여 복제

## 1. 브랜치 전략

```
main          → 배포/릴리즈 (직접 push 금지)
develop       → 통합 브랜치
feature/xxx   → 기능 개발
fix/xxx       → 버그 수정
refactor/xxx  → 리팩토링
docs/xxx      → 문서
chore/xxx     → 빌드/툴링
```

## 2. 커밋 메시지 컨벤션 (Conventional Commits)

```
<type>(<scope>): <subject>

[body - 선택사항]
왜 이 변경이 필요한가? (what/why, not how)

[footer - 선택사항]
Refs: #이슈번호
BREAKING CHANGE: 설명
```

**타입**: `feat` `fix` `refactor` `docs` `chore` `test` `perf` `ci`

## 3. Git Worktree 활용

병렬 작업을 위한 worktree 사용 — 브랜치 전환 없이 여러 작업 동시 진행:

```bash
git worktree add ../project-feature feature/new-feature
git worktree list
git worktree remove ../project-feature
```

> WSL2: worktree 경로는 반드시 `/home/` 하위 — `/mnt/c/` 경로 사용 시 성능 저하 및 심볼릭 링크 문제

## 4. Git Hooks 자동화

**pre-commit** (커밋 전 자동 검사):
```bash
#!/bin/sh
# CRLF 라인 엔딩 감지
if git diff --cached --check | grep -q 'CRLF'; then
    echo "CRLF 감지. dos2unix 실행 후 재시도"; exit 1
fi
# 디버그 코드 감지
if git diff --cached | grep -qE '^\+.*(printf\s*\(|DEBUG_PRINT)'; then
    echo "디버그 출력 감지. 의도적이면 --no-verify"; exit 1
fi
```

**commit-msg** (메시지 형식 검사):
```bash
#!/bin/sh
PATTERN='^(feat|fix|refactor|docs|chore|test|perf|ci)(\(.+\))?: .{1,72}'
if ! grep -qE "$PATTERN" "$1"; then
    echo "형식 오류: <type>(<scope>): <subject>"; exit 1
fi
```

상세 자동화 스크립트 패턴은 `blueprints/git-workflow.md` 5절 참조.
