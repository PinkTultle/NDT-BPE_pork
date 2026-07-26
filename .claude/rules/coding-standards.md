---
description: "C/C++ 임베디드 코딩 표준 — 네이밍, 헤더 가드, 안전한 코딩, 에러 처리"
paths:
  - "**/*.c"
  - "**/*.cpp"
  - "**/*.h"
  - "**/*.hpp"
---

# C/C++ 임베디드 코딩 표준

> 원본: `blueprints/coding-standards.md`
> 프로젝트 rules 템플릿 — `/adt:project-configure`가 프로젝트에 맞게 커스터마이즈하여 복제

## 1. 기본 컨벤션

- 인덴트: 4 spaces (탭 금지)
- 줄 길이: 100자 이하 권장, 120자 하드 제한
- 인코딩: UTF-8, LF 줄바꿈 (CRLF 절대 금지 — WSL2 특히 주의)

## 2. 네이밍 규칙

```c
// 전처리기 매크로: ALL_CAPS_SNAKE
#define MAX_BUFFER_SIZE  256

// 타입/구조체: PascalCase (typedef 포함)
// 참고: _t suffix는 POSIX 예약어와 충돌 가능 — 프로젝트 컨벤션으로 의도적 사용
typedef struct MotorStatus { ... } MotorStatus_t;

// 함수: lower_snake_case, 모듈_동사_목적어 형식
uint8_t motor_get_speed(MotorHandle_t *handle);

// 전역 변수: g_ 접두사
static uint32_t g_tick_count = 0;

// 로컬 변수: lower_snake_case
uint8_t read_val = 0;

// 상수/열거형: k_ 접두사 또는 ALL_CAPS
const uint32_t kTimeoutMs = 1000;
```

## 3. 헤더 가드

```c
#ifndef MODULE_NAME_H_
#define MODULE_NAME_H_

#ifdef __cplusplus
extern "C" {
#endif

/* 내용 */

#ifdef __cplusplus
}
#endif

#endif /* MODULE_NAME_H_ */
```

## 4. 안전한 코딩 (임베디드 필수)

- `malloc` / `free` 동적 메모리 할당: 힙이 제한된 환경에서는 사용 금지, 사용 시 반드시 경고
- 포인터 역참조 전 NULL 체크 필수
- 정수 오버플로우 주의 — 명시적 캐스팅 사용
- 인터럽트 핸들러 내: `volatile` 변수, 최소한의 코드만
- 재진입 불가 함수는 헤더에 `/* NOT REENTRANT */` 명시

## 5. 에러 처리 패턴

```c
// 반환값 기반 에러 처리 (예외 없는 C 환경)
typedef enum {
    ERR_OK    = 0,
    ERR_PARAM = -1,
    ERR_TIMEOUT = -2,
} ErrorCode_t;

ErrorCode_t do_something(uint8_t *buf, size_t len) {
    if (buf == NULL || len == 0) return ERR_PARAM;
    /* ... */
    return ERR_OK;
}
```
