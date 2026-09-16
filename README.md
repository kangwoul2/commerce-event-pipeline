# Commerce Event Pipeline

Spring Boot로 받은 구매 이벤트를 Kafka로 전달하고, 소비자가 PostgreSQL의 지역별 매출에 안전하게 반영하도록 만든 이벤트 기반 백엔드입니다.

초기 프로젝트에서는 SQL과 Excel로 지역, 상품, 기온 데이터를 결합해 판매 패턴을 분석했습니다. 이후 실제 서비스에서는 분석용 데이터가 어떻게 생성되고, 중복 전달이나 동시 처리 상황에서도 집계 결과를 어떻게 안전하게 유지할 수 있는지로 문제를 확장했습니다.

> 핵심 질문: **같은 구매 이벤트가 여러 번 전달되어도 매출 집계에는 한 번만 반영하려면 어떻게 설계해야 하는가?**

---

## 1. 전체 구조

```text
클라이언트
  ↓
POST /api/v1/purchases
Idempotency-Key: UUID
  ↓
Spring Boot API
  ↓ 이벤트 발행
Kafka purchase-events
  ↓
PurchaseEventConsumer
  ↓
집계 처리 서비스
  ├─ processed_events: 중복 확인
  └─ regional_sales: 지역별 매출 집계
  ↓
PostgreSQL
```

구매 API는 최종 집계 완료까지 기다리지 않고 `202 Accepted`를 반환합니다. 실제 지역별 매출 반영은 Kafka 소비자가 비동기로 처리합니다.

---

## 2. 왜 Kafka를 사용했는가

단순히 대용량이라는 이유로 Kafka를 사용한 것은 아닙니다.

구매 요청과 분석 집계를 같은 API 안에서 처리하면:

- 사용자 요청 지연시간이 집계 작업과 결합됨
- 분석, 알림, 추천 기능이 늘수록 구매 API 책임이 커짐
- 후속 처리 장애가 사용자 요청에 영향을 줄 수 있음

Kafka를 사용하면 하나의 구매 이벤트를 여러 소비자가 독립적으로 처리할 수 있고, 이벤트를 다시 읽을 수도 있습니다.

```text
구매 API
  ↓
Kafka
  ├─ 매출 집계 소비자
  ├─ 알림 소비자
  └─ 분석 소비자
```

현재 프로젝트에서는 지역별 매출 집계 소비자를 구현했습니다.

---

## 3. 왜 `202 Accepted`를 반환하는가

HTTP 응답 시점에는 구매 이벤트를 받아 Kafka로 보내는 단계까지 진행하지만, 지역별 매출 집계가 이미 끝났다고 보장하지 않습니다.

따라서 최종 작업 완료를 의미하는 응답보다 **요청은 받아들였지만 후속 처리는 진행 중이라는 의미의 `202 Accepted`**가 더 정확합니다.

---

## 4. 최소 한 번 전달과 중복 문제

Kafka 소비자는 상황에 따라 같은 이벤트를 다시 받을 수 있습니다.

대표적인 경우:

```text
이벤트 A 소비
  ↓
DB 반영 완료
  ↓
오프셋 커밋 전에 소비자 장애
  ↓
Kafka가 이벤트 A 재전달
```

따라서 **소비자가 한 번만 호출될 것이라고 가정하지 않고, 같은 이벤트가 다시 와도 최종 DB 결과가 한 번만 반영되도록 설계**했습니다.

---

## 5. 멱등 소비자

이벤트마다 고유한 `eventId`를 사용합니다.

```text
이벤트 도착
  ↓
processed_events에 event_id 저장 시도
  ├─ 처음 본 ID → 지역별 매출 증가
  └─ 이미 있는 ID → 아무 작업 없이 종료
```

`processed_events.event_id`에는 기본키 또는 유일성 제약조건이 있으므로 여러 서버가 동시에 같은 ID를 처리하려 해도 DB가 최종 중복 여부를 판단합니다.

### 왜 메모리 `Set`을 사용하지 않았는가

프로세스 메모리는:

- 서버가 여러 대면 서로 공유되지 않음
- 서버 재시작 시 기록이 사라짐

따라서 PostgreSQL을 중복 처리 여부의 기준 저장소로 사용합니다.

---

## 6. 생산자 멱등성과 소비자 멱등성은 다른 문제

Kafka 생산자 설정에 `enable.idempotence=true`가 있어도 소비자 쪽 DB 멱등성이 필요합니다.

```text
생산자 멱등성
→ 생산자 재시도로 같은 Kafka 기록이 중복 생성되는 문제를 줄임

소비자 멱등성
→ 같은 Kafka 이벤트가 다시 전달되어도 DB 결과가 중복 반영되지 않게 함
```

즉 생산자 설정 하나로 Kafka부터 외부 DB까지 전체가 정확히 한 번 처리되는 것은 아닙니다.

Kafka의 재전달 자체를 없애려고 하지 않고, 같은 이벤트가 다시 와도 최종 DB 결과가 중복되지 않도록 소비자 멱등성을 보장했습니다.

---

## 7. 중복 기록과 실제 집계를 왜 같은 트랜잭션으로 묶는가

두 작업은 하나의 `@Transactional` 범위에서 처리합니다.

```text
BEGIN
  processed_events에 event_id 저장
  regional_sales 증가
COMMIT
```

만약 중복 처리 기록만 먼저 저장되고 매출 집계가 실패하면 다음 재시도에서는 이미 처리된 이벤트로 판단해 실제 매출이 영원히 누락될 수 있습니다.

그래서 **중복 확인 기록과 실제 집계를 함께 성공하거나 함께 롤백**하도록 묶었습니다.

---

## 8. 동시 집계와 갱신 손실

좋지 않은 방식:

```text
SELECT total
애플리케이션에서 total += amount
UPDATE total
```

동시에 두 요청이 같은 값을 읽으면 한쪽 증가분이 사라질 수 있습니다.

그래서 PostgreSQL에서 현재 값에 금액을 더하는 **원자적 갱신**을 사용합니다.

```text
DB 내부에서
현재 총액 + 새 금액
→ 한 문장으로 갱신
```

공유 숫자는 가능한 한 애플리케이션으로 읽어온 뒤 다시 저장하지 않고 DB가 직접 원자적으로 수정하도록 합니다.

---

## 9. 파티션 키를 지역으로 둔 이유

Kafka는 전체 주제의 순서를 보장하는 것이 아니라 **같은 파티션 내부의 순서**를 보장합니다.

이 프로젝트에서는 주요 집계 단위가 지역이므로 `region`을 메시지 키로 사용합니다.

```text
New York 이벤트 → 같은 키 → 같은 파티션
Texas 이벤트    → 같은 키 → 같은 파티션
```

장점:
- 같은 지역 이벤트의 상대적 순서를 유지하기 쉬움

단점:
- 특정 지역에 트래픽이 몰리면 한 파티션에 부하가 집중될 수 있음

실제 지역별 트래픽 편차가 크다면 `region + bucket`처럼 키를 나누는 방식을 검토할 수 있지만, 그 경우 지역 전체의 엄격한 순서는 약해질 수 있습니다.

---

## 10. Redis 분산 락을 사용하지 않은 이유

현재 문제는 여러 서버가 동시에 임계 구역에 들어가는 것 자체보다 **같은 이벤트의 최종 결과가 두 번 반영되는 것**입니다.

이 문제에는:

```text
이벤트 ID
→ DB 유일성 제약조건
→ 하나의 트랜잭션
```

이 더 직접적인 해결책입니다.

분산 락은 DB 제약조건과 멱등성만으로 해결하기 어려운 별도의 분산 조정 문제가 있을 때 검토합니다.

---

## 11. 장애 상황

### DB 반영 후 오프셋 커밋 전에 장애

같은 이벤트가 다시 전달될 수 있지만 `processed_events`가 중복 반영을 막습니다.

### Kafka 발행 실패

Kafka 발행 결과를 확인한 뒤 `202`를 반환합니다. 발행 실패나 대기 시간 초과는 `503`으로 응답합니다. 시간 초과 시 실제 발행 여부가 불확실할 수 있으므로 재요청에는 같은 `Idempotency-Key`와 본문을 사용해야 합니다.

실제 주문 DB 저장과 Kafka 이벤트 발행을 반드시 함께 보장해야 하는 서비스라면 **트랜잭셔널 아웃박스**를 검토할 수 있습니다. 현재 프로젝트는 전체 주문 시스템이 아니므로 구현한 기능처럼 과장하지 않습니다.

### 반복해도 해결되지 않는 잘못된 메시지

현재는 오류가 난 메시지를 건너뛰지 않도록 소비를 중단합니다. 원인을 해결한 뒤 애플리케이션을 재시작하면 미커밋 메시지를 다시 읽습니다. 그동안 정상 메시지의 집계도 지연될 수 있습니다. 형식 오류나 업무 규칙 위반은 원인을 해결하지 않고 재시작해도 다시 실패합니다.

재시도 횟수를 제한하고 DLQ로 분리하는 방식은 후속 개선 항목입니다. 현재는 DLQ 운영 구성을 구현하지 않았습니다. Actuator의 기본 `health`만으로 소비 중단을 감지하는 구성도 아직 없으므로 소비자 오류 로그와 지연량을 별도로 확인해야 합니다.

---

## 12. 중복 재전달 실험

`experiments/replay_duplicate.py`는 같은 이벤트 ID를 여러 번 보내도 최종 집계가 한 번만 증가하는지 확인합니다.

```text
같은 eventId를 N번 전송
  ↓
수신 요청 수: N
실제 매출 반영: 1회
```

확인 항목:

- 반복 요청 수
- `processed_events` 행 수
- `regional_sales` 증가량
- 소비자 오류

스크립트는 반복 요청 수와 실험 전용 지역의 건수·금액을 자동 검사합니다. `processed_events` 행 수와 소비자 오류 로그는 아래 실행 기록의 명령으로 별도 확인합니다.

측정하지 않은 처리량 수치를 성과로 작성하지 않습니다.

---

## 13. 기술별 역할

| 역할 | 기술 | 이유 |
|---|---|---|
| API | Java 17 / Spring Boot | 구매 이벤트 수신과 조회 |
| 이벤트 전달 | Kafka / Spring Kafka | 생산자와 소비자 분리, 재전달 가능 |
| 기준 저장소 | PostgreSQL | 중복 처리 기록과 지역별 집계 |
| DB 변경 이력 | Flyway | 스키마 변경 관리 |
| 입력 검증 | Jakarta Validation | API 요청 검증 |
| 상태 확인 | Spring Actuator | 기본 상태와 운영 정보 |
| 실행 환경 | Docker Compose | 로컬 Kafka/PostgreSQL 구성 |

---

## 14. 현재 한계

- 실제 결제 승인 기능 없음
- 재고 정합성 처리 없음
- 트랜잭셔널 아웃박스 미구현
- DLQ 운영 정책 미완성
- 실제 운영용 다중 Kafka 브로커 구성 미구축
- 대규모 처리량 실측 미완료

구현하지 않은 기능은 사용했다고 표현하지 않습니다.

---

## 설계 원칙

> **분산 시스템에서는 중복 전달과 중간 장애를 예외 상황이 아니라 정상적으로 발생할 수 있는 경로로 보고 설계합니다.**

이 프로젝트의 핵심은 Kafka 자체가 아니라 **재전달, 동시 갱신, 트랜잭션 경계를 어떻게 안전하게 처리했는지**에 있습니다.

---

## 개인 공부 기록 — 파일을 따라가며 정리하기

이 부분은 코드를 다시 읽을 때 참고할 개인 필기다. 클래스 이름을 외우기보다 **이 파일이 왜 필요하고, 빠지면 어떤 문제가 생기는지**를 설명하는 것이 목표다. 설계 의도, 테스트로 확인한 사실, 아직 확인하지 못한 부분은 구분해서 적는다.

### 1. 먼저 요청 한 건을 따라가 보기

예를 들어 서울에서 10,000원 구매가 발생했다고 생각한다.

1. 호출자가 구매 한 건을 구분할 UUID를 `Idempotency-Key` 헤더에 넣는다. 같은 구매를 재시도할 때는 이 값을 유지한다.
2. `PurchaseController`가 요청을 검증하고 `PurchaseEvent`로 바꾼다.
3. `PurchaseEventPublisher`가 지역을 키로 Kafka에 보낸다. 발행 성공을 확인한 뒤 API가 `202`를 응답한다.
4. `PurchaseEventConsumer`가 메시지를 받아 `PurchaseProjectionService`에 넘긴다.
5. 집계 서비스가 이벤트 ID 저장과 서울 매출 증가를 하나의 DB 트랜잭션으로 처리한다.
6. 조회 API는 현재 DB에 반영된 결과를 읽는다. 구매 응답 직후에는 아직 이전 값일 수 있다.

여기서 비동기라는 말은 **HTTP 요청이 매출 집계 완료까지 기다리지 않는다**는 뜻이다. Kafka 발행 성공을 기다리는 것과 DB 집계 완료를 기다리는 것은 서로 다른 단계다.

### 2. 실행과 설정 파일

#### [`service/pom.xml`](service/pom.xml) — 무엇을 가져와서 빌드하는가

Maven이 의존성과 빌드 방법을 읽는 파일이다. Java 버전은 소스와 바이트코드의 기준이고, Spring Boot 부모 설정은 라이브러리 버전을 맞춰 주는 역할을 한다.

웹은 HTTP 요청, 검증은 잘못된 입력 차단, JPA는 DB 접근, Kafka는 이벤트 전달, Flyway는 테이블 변경 이력을 담당한다. Actuator는 상태 확인에 사용한다. 라이브러리를 목록에 넣었다고 자동 설정까지 항상 켜지는 것은 아니다. 특히 Spring Boot 4에서는 Kafka와 Flyway의 자동 설정을 포함한 스타터 구성을 확인해야 한다.

`mvn verify` 성공도 서비스 전체 정상 동작과 같은 뜻은 아니다. 어떤 테스트가 실행됐는지, 실제 DB와 Kafka를 사용했는지까지 봐야 한다.

#### [`docker-compose.yml`](docker-compose.yml) — 로컬에서 필요한 기반 시설

PostgreSQL과 Kafka를 띄운다. Java 애플리케이션은 이 파일에 포함되지 않으므로 따로 실행한다. `5435:5432`는 내 PC의 5435번 포트를 DB 컨테이너의 5432번 포트에 연결한다는 뜻이다.

Kafka의 `advertised.listeners`는 접속한 클라이언트에게 알려 줄 주소다. 현재 `localhost:9092`는 Java를 내 PC에서 실행하는 구성이다. 나중에 Java도 컨테이너에 넣으면 컨테이너 안의 `localhost`는 자기 자신이므로 주소 구성을 바꿔야 한다.

브로커와 컨트롤러를 한 컨테이너에서 실행하고 복제본도 하나만 둔다. 개발 실습에는 간단하지만, 브로커 장애를 다른 서버가 대신 처리하는 고가용성 구성은 아니다. 영속 볼륨을 명시하지 않았으므로 컨테이너를 교체해도 데이터가 보존된다고 가정하면 안 된다.

#### [`application.yml`](service/src/main/resources/application.yml) — 실행 중 적용할 설정

`${DATABASE_URL:기본값}` 형태는 환경변수가 있으면 그 값을 쓰고 없으면 기본값을 사용한다. 저장소의 기본 계정은 로컬 실습용이다.

`ddl-auto: validate`는 JPA가 테이블을 만드는 설정이 아니다. Flyway가 먼저 테이블을 만들고 JPA는 엔티티와 맞는지 검사한다. `open-in-view: false`는 HTTP 응답을 만드는 단계까지 DB 영속성 컨텍스트를 열어 두지 않겠다는 설정이다.

생산자의 `enable.idempotence`는 Kafka 전송 재시도의 중복을 다룬다. 사용자가 같은 구매를 API로 두 번 보낸 것을 구분하는 기능은 아니다. `acks: all`도 현재 복제본이 하나라면 여러 서버에 복제했다는 의미가 되지 않는다.

소비자의 `earliest`는 매번 처음부터 읽으라는 뜻이 아니다. 소비자 그룹의 유효한 커밋 위치가 없을 때 적용된다. 오프셋은 그 그룹이 어디까지 읽었는지 나타내는 위치다.

Actuator 노출 목록에는 실제 제공하는 `health`, `info`, `metrics`를 적었다. 예전 설정의 `prometheus`는 전용 지표 등록기 의존성 없이 이름만 적혀 있어 제외했다. 노출 목록에 이름을 넣는 것만으로 해당 기능이 생기는 것은 아니다.

#### [`CommerceEventsApplication.java`](service/src/main/java/dev/kangwoul/commerce/CommerceEventsApplication.java) — 시작점

`main`에서 Spring Boot를 시작한다. `@SpringBootApplication`이 하위 패키지의 컨트롤러, 서비스, 설정 등을 찾아 빈으로 등록한다. 빈은 스프링이 생성하고 의존성을 연결해 주는 객체라고 이해했다.

#### [`KafkaTopicConfig.java`](service/src/main/java/dev/kangwoul/commerce/config/KafkaTopicConfig.java) — 토픽 구성

`purchase-events` 토픽을 파티션 6개, 복제본 1개로 선언한다. 파티션은 메시지를 나눠 저장하고 소비 작업을 분담하는 단위다. **파티션이 6개라고 지금 소비자 스레드도 6개인 것은 아니다.** 같은 그룹의 소비자 수나 동시 실행 설정을 별도로 늘려야 한다.

지역을 키로 쓰면 같은 지역은 같은 파티션으로 배치된다. 단, 파티션 수를 바꾸면 키의 배치도 달라질 수 있다. 현재 매출 계산은 덧셈이므로 순서보다 중복 방지와 증가분 보존이 핵심이다.

### 3. 구매 요청을 받는 파일

#### [`PurchaseRequest.java`](service/src/main/java/dev/kangwoul/commerce/purchase/PurchaseRequest.java) — API 입력 계약

클라이언트가 보내는 고객, 상품 분류, 지역, 금액을 받는 객체다. `record`는 이처럼 값을 전달하는 객체를 간결하게 선언하는 문법이다.

`@NotBlank`는 문자열의 빈 값과 공백만 있는 값을 막고, `@NotNull`은 누락된 금액을 막는다. 금액은 `double` 대신 `BigDecimal`로 다룬다. 십진수 금액을 이진 부동소수점으로 계산할 때 생기는 오차를 피하려는 선택이다. DB의 지역 길이와 금액 소수 자릿수도 API 검증과 맞아야 한다. 그렇지 않으면 API가 수락한 요청이 나중에 DB에서 실패하거나 반올림될 수 있다.

#### [`PurchaseController.java`](service/src/main/java/dev/kangwoul/commerce/purchase/PurchaseController.java) — HTTP와 업무 처리의 연결

`@RequestHeader`로 UUID를 받고 `@Valid`로 본문 검증을 실행한다. 검증 애너테이션만 선언하고 실제 검증을 실행하지 않으면 의미가 없다. 컨트롤러에서는 이벤트를 만들고 발행을 요청한다. 매출 계산이나 중복 판정은 여기에서 하지 않는다.

`Instant.now()`는 이 서버가 요청을 받은 시각에 가깝다. 실제 결제 승인 시각을 외부에서 받은 것이 아니다. 같은 키로 재요청하면 새 이벤트 객체와 시각이 생기지만, 소비자는 이벤트 ID 기준으로 중복을 판단한다.

현재 `Idempotency-Key`는 **집계 중복 방지에 사용할 ID**다. 최초 HTTP 응답을 저장해 그대로 돌려주는 기능이나, 같은 키에 다른 금액을 보내면 `409`로 거절하는 기능은 없다. 같은 ID로 다른 내용을 보내면 먼저 DB에 반영된 내용만 남을 수 있으므로 같은 구매의 재시도에는 같은 키와 본문을 사용해야 한다.

#### [`PurchaseEvent.java`](service/src/main/java/dev/kangwoul/commerce/purchase/PurchaseEvent.java) — Kafka로 전달하는 데이터

요청 데이터에 이벤트 ID와 발생 시각을 더한 전달 형식이다. DB 엔티티가 아니며, 현재는 구매 원장을 별도 테이블에 저장하지 않는다. 고객과 상품 분류도 이벤트에는 있지만 지역별 집계 테이블에는 보관하지 않는다.

입력 객체와 이벤트 객체를 구분하면 API 입력 방식과 소비자 계약을 따로 생각할 수 있다. 다만 이벤트 필드를 바꾸면 이미 Kafka에 저장된 옛 메시지도 읽을 수 있는지 확인해야 한다.

#### [`PurchaseEventPublisher.java`](service/src/main/java/dev/kangwoul/commerce/purchase/PurchaseEventPublisher.java) — 발행 성공 여부를 확인하는 곳

`KafkaTemplate.send(토픽, 키, 이벤트)`에서 키는 지역이다. 반환값은 발행 작업이 나중에 성공하거나 실패했음을 알려 주는 객체다. 호출했다고 곧바로 Kafka 저장이 끝난 것은 아니다.

처음 코드에서는 이 결과를 확인하지 않아 발행 실패와 `202` 응답이 엇갈릴 수 있었다. README의 약속을 지키려면 성공 확인 뒤 응답하고, 실패나 제한 시간 초과는 실패 응답으로 연결해야 한다. 단, 시간 초과는 **발행 결과를 확인하지 못했다**는 뜻이지 Kafka에 절대 기록되지 않았다는 뜻은 아니다. 재시도할 때 같은 이벤트 ID를 유지해야 하는 이유다.

현재는 발행 결과를 최대 10초 기다린다. 단, `send()` 호출 자체도 메타데이터나 버퍼를 기다릴 수 있어서 `max.block.ms`를 별도로 5초로 제한했다. 두 설정 때문에 HTTP 요청 전체의 상한이 정확히 10초라고 설명하면 안 된다.

#### [`PurchasePublishException.java`](service/src/main/java/dev/kangwoul/commerce/purchase/PurchasePublishException.java), [`PurchaseExceptionHandler.java`](service/src/main/java/dev/kangwoul/commerce/purchase/PurchaseExceptionHandler.java) — 내부 실패를 HTTP 응답으로 연결

발행 클래스는 발행 실패를 예외로 전달하고, 공통 예외 처리기가 `503` 응답으로 바꾼다. 발행 코드가 HTTP 응답 생성까지 맡지 않도록 역할을 나눴다. 상세 원인은 서버 로그에 남기고, 호출자에게는 같은 키와 본문으로 재시도하라는 안내를 준다. 스레드가 중단된 경우에는 중단 표시도 복원한다.

### 4. 소비와 DB 집계를 담당하는 파일

#### [`PurchaseEventConsumer.java`](service/src/main/java/dev/kangwoul/commerce/projection/PurchaseEventConsumer.java) — 메시지를 집계 서비스로 전달

`@KafkaListener`가 Kafka 메시지를 받아 메서드를 호출한다. `regional-sales-projection`은 소비자 그룹 이름이다. 같은 그룹의 서버들은 파티션을 나눠 처리하고, 다른 그룹은 같은 토픽을 자기 진행 위치에 따라 별도로 읽을 수 있다.

집계 오류를 잡아서 조용히 정상 종료하면 처리에 성공한 것처럼 오프셋이 진행될 수 있다. 실패는 오류 처리 흐름으로 전달되어야 한다. DB 커밋과 Kafka 오프셋 커밋은 별개이므로, 그 사이 장애가 나면 이미 반영한 이벤트가 다시 올 수 있다.

#### [`KafkaConsumerConfig.java`](service/src/main/java/dev/kangwoul/commerce/config/KafkaConsumerConfig.java) — 소비 실패 시 처리 정책

DLQ가 없는 상태에서 실패 메시지를 건너뛰지 않도록 소비 중단 처리기를 등록했다. `application.yml`의 자동 커밋 비활성화와 레코드 단위 확인 설정도 함께 읽는다. 정상 처리가 끝난 메시지의 위치는 커밋하지만 실패한 메시지는 재시작 뒤 다시 처리할 수 있게 남겨 둔다.

장점은 오류를 조용히 누락시키지 않는 것이고, 대가는 집계가 멈춰 운영자가 원인 해결과 재시작을 해야 한다는 것이다. 단순히 “재시도하면 된다”라고 설명하지 않는다. Kafka 보관 기간이 지나기 전에 복구해야 하며, 기본 상태 확인 API만으로 이 중단을 감지하는 기능은 아직 없다.

#### [`PurchaseProjectionService.java`](service/src/main/java/dev/kangwoul/commerce/projection/PurchaseProjectionService.java) — 정합성을 지키는 중심

여기서 집계란 이벤트를 조회하기 좋은 지역별 숫자로 바꾸어 저장하는 작업이다. `tryInsert` 결과가 1이면 처음 처리하는 ID이므로 매출을 올리고, 0이면 이미 처리된 ID이므로 종료한다.

`@Transactional`은 ID 기록과 매출 갱신을 한 묶음으로 만든다. 매출 갱신이 실패하면 ID 기록도 되돌아가야 다음 재시도가 가능하다. `true`와 `false`는 신규 반영 여부이지 HTTP 성공/실패 상태가 아니다. 중복을 건너뛰는 것도 정상 처리다.

스프링이 관리하는 서비스 객체를 통해 호출해야 트랜잭션이 적용된다. 테스트에서 `new PurchaseProjectionService(...)`로 생성하면 분기 로직은 검사할 수 있어도 실제 트랜잭션 검증은 할 수 없다.

#### [`ProcessedEvent.java`](service/src/main/java/dev/kangwoul/commerce/projection/ProcessedEvent.java) — 처리 완료 ID의 DB 매핑

JPA 엔티티는 테이블과 자바 객체를 연결한다. `@Id`는 기본 키를 뜻하고, `eventId`는 기본 이름 변환 규칙에 따라 `event_id` 컬럼에 연결된다. 인자 없는 생성자는 JPA가 객체를 만들 때 필요하다.

이 테이블은 주문 내역이 아니라 중복 판정 기록이다. 기록을 삭제한 뒤 옛 이벤트를 재전달하면 다시 집계될 수 있다. 보관 기간은 Kafka에서 얼마나 과거까지 재처리할 것인지와 함께 정해야 한다.

#### [`ProcessedEventRepository.java`](service/src/main/java/dev/kangwoul/commerce/projection/ProcessedEventRepository.java) — 조회 후 저장 대신 바로 저장 시도

`INSERT ... ON CONFLICT DO NOTHING`은 기본 키가 충돌하면 오류 대신 삽입을 생략한다. 먼저 `exists`로 확인한 뒤 저장하면 두 요청이 동시에 “없다”고 판단할 수 있으므로 DB 제약조건을 최종 기준으로 삼는다.

`@Modifying`은 데이터를 바꾸는 쿼리라는 표시이고, 트랜잭션 시작은 서비스가 맡는다. 동시에 같은 ID를 넣으면 DB가 충돌을 조정한다. 먼저 처리한 트랜잭션이 롤백되면 다른 요청이 삽입에 성공할 수도 있다.

#### [`RegionalSales.java`](service/src/main/java/dev/kangwoul/commerce/projection/RegionalSales.java) — 조회용 지역별 결과

지역 하나에 주문 수와 누적 금액 하나가 대응한다. 여기서 주문 수는 실제 결제 승인 건수가 아니라 **집계된 서로 다른 이벤트 ID 수**다. 별도의 주문·결제 검증은 없다.

현재는 엔티티를 조회 응답으로 바로 반환한다. 작은 실습에는 간단하지만 테이블 구조 변경이 응답 형식에도 영향을 주므로, API 계약이 커지면 조회 응답 객체를 분리할 수 있다.

#### [`RegionalSalesRepository.java`](service/src/main/java/dev/kangwoul/commerce/projection/RegionalSalesRepository.java) — 없는 지역은 생성, 있는 지역은 증가

`INSERT ... ON CONFLICT (region) DO UPDATE`는 신규 행 삽입과 기존 행 갱신을 한 문장으로 처리한다. 현업에서 흔히 업서트라고 부른다. `EXCLUDED.total_amount`는 이번에 넣으려던 금액이다.

기존 DB 값에 이번 금액을 직접 더하므로 자바에서 같은 총액을 읽고 서로 덮어쓰는 갱신 손실을 피한다. 같은 지역 행을 동시에 수정하면 DB 잠금 때문에 대기할 수 있다. **원자적 갱신이 잠금이나 병목이 없다는 뜻은 아니다.**

#### [`AnalyticsController.java`](service/src/main/java/dev/kangwoul/commerce/projection/AnalyticsController.java) — 현재 반영된 집계 조회

`GET /api/v1/analytics/regions`는 현재 DB에 있는 지역별 결과를 반환한다. Kafka에 접수만 되고 아직 소비되지 않은 이벤트는 포함하지 않는다. 별도 정렬이 없어 응답 배열 순서를 고정이라고 가정하면 안 된다. 데이터가 커지면 페이지 조회도 필요하다.

#### [`V1__event_projection.sql`](service/src/main/resources/db/migration/V1__event_projection.sql) — DB 규칙의 출발점

이벤트 ID와 지역에 기본 키를 두고, 주문 수와 금액에 음수 방지 제약조건을 둔다. `NUMERIC(20,2)`는 전체 20자리 중 소수 2자리를 사용하는 금액 형식이다. 한 건의 금액이 유효해도 누적 총액은 언젠가 컬럼 범위를 넘을 수 있다.

Flyway는 적용한 버전과 검사값을 기록한다. 이미 적용한 `V1`을 수정하기보다 새 변경은 `V2__...sql`로 추가해야 한다. API 검증은 사용자에게 빨리 오류를 알려 주고, DB 제약조건은 다른 경로의 잘못된 쓰기도 막는다. 두 검증의 역할이 다르다.

### 5. 테스트·실험·문서 파일을 읽는 기준

#### [`PurchaseProjectionServiceTest.java`](service/src/test/java/dev/kangwoul/commerce/projection/PurchaseProjectionServiceTest.java) — 분기 확인과 DB 검증 구분

가짜 저장소를 사용하는 단위 테스트다. 처음 테스트는 `tryInsert`가 0을 돌려준다고 미리 정해 놓고 매출 갱신을 호출하지 않는지만 확인했다. 실제로 같은 ID를 DB에 두 번 넣은 테스트는 아니었다.

중복 분기, 신규 반영 분기, 예외 전파는 단위 테스트로 빠르게 확인할 수 있다. SQL 문법, 기본 키 충돌, 롤백, 동시 갱신은 실제 PostgreSQL을 사용하는 통합 테스트가 필요하다. 테스트 이름만 보고 검증 범위를 넓게 설명하지 않기로 했다.

#### 추가한 검증 파일

- [`PurchaseEventPublisherTest.java`](service/src/test/java/dev/kangwoul/commerce/purchase/PurchaseEventPublisherTest.java): Kafka 성공 응답 전에는 반환하지 않는지, 동기·비동기 실패와 시간 초과를 실패로 전달하는지 검사한다. 실제 브로커 장애를 일으킨 테스트는 아니다.
- [`PurchaseControllerTest.java`](service/src/test/java/dev/kangwoul/commerce/purchase/PurchaseControllerTest.java): 모의 HTTP 요청으로 `202`, `503`, 잘못된 키·금액·지역의 `400`을 확인한다. 잘못된 입력이 발행까지 도달하지 않는지도 확인한다.
- [`PurchaseEventSerializationTest.java`](service/src/test/java/dev/kangwoul/commerce/purchase/PurchaseEventSerializationTest.java): 실제 설정에 적힌 JSON 변환기를 사용해 이벤트를 바이트로 바꿨다가 복원한다. UUID, 한글, 금액, 시각이 유지되는지 확인한다. Spring Boot 4의 Jackson 3과 이전 JSON 변환기의 버전 불일치를 놓치지 않기 위한 테스트다.
- [`PurchasePipelineIT.java`](service/src/test/java/dev/kangwoul/commerce/projection/PurchasePipelineIT.java): 실제 Kafka·PostgreSQL 환경에서 중복, 롤백 후 재시도, 같은 ID의 동시 처리, 서로 다른 ID의 동시 매출 증가, HTTP부터 조회까지의 연결을 검사한다. 일반 `mvn verify`와 구분해 `mvn -Pintegration verify`로 실행한다. 이 테스트도 프로세스 강제 종료나 브로커 장애 복구까지 검증하지는 않는다.

#### [`experiments/replay_duplicate.py`](experiments/replay_duplicate.py) — API부터 집계까지 확인하는 실험

같은 ID의 요청을 반복해서 보내는 실험이다. `202`가 여러 번 나오는 것만으로 중복 방지 성공이라고 판단하면 안 된다. 비동기 집계가 따라올 때까지 기다린 뒤 건수와 금액이 한 번만 증가했는지 확인해야 한다.

다른 사용자의 구매와 섞이지 않도록 실험 전용 지역을 사용한다. 이 실험은 HTTP 재요청에 대한 확인이며, 소비자 프로세스를 강제로 종료하거나 Kafka 오프셋을 되돌리는 장애 실험과는 구분한다. 제한된 관찰 시간이므로 이후에도 중복이 절대 발생하지 않는다는 증명은 아니다.

#### [`.github/workflows/service-ci.yml`](.github/workflows/service-ci.yml) — GitHub에서 반복 검증

CI는 변경할 때마다 같은 빌드와 테스트를 자동 실행하는 장치다. 이 파일은 Java 환경을 준비하고 Maven 검증을 실행한다. 초록색 결과를 볼 때 테스트가 실제로 실행됐는지, 건너뛴 테스트는 없는지도 확인해야 한다.

#### 문서와 원본 자료

- [`docs/ARCHITECTURE_DECISIONS.md`](docs/ARCHITECTURE_DECISIONS.md): 왜 이 방식을 골랐는지와 대가를 정리한 문서다. 구현 위치와 테스트 근거를 같이 찾아 읽는다.
- [`docs/INTERVIEW_GUIDE.md`](docs/INTERVIEW_GUIDE.md): 설명을 연습하는 보조 자료다. 예시 답변의 “확인했다”는 문장은 실제 실행 근거가 있을 때만 내 경험으로 말한다.
- [`docs/assets/event_flow.svg`](docs/assets/event_flow.svg): 요청부터 집계까지 그린 그림이다. 실행 코드가 아니며 구현이 바뀌면 설명도 함께 맞춰야 한다.
- `e-commerce_data.xlsx`, `add_data_analysis.xlsx`, `4팀 1차 프로젝트 최종.pptx`: 초기 분석 자료다. 현재 Java 코드가 이 파일을 읽어 이벤트로 변환하는 기능은 없다. 문서 내부의 수식이나 분석 결과를 검증한 것으로 설명하지 않는다.
- `service/target/`: 빌드로 생성되는 클래스·실행 파일·테스트 결과다. 소스가 아니므로 Git에 올리지 않는다.
- [`.gitignore`](.gitignore): 빌드 결과, Python 캐시, 로컬 환경 파일 등을 실수로 커밋하지 않도록 제외한다. 파일을 실제로 삭제하거나 이미 추적 중인 파일을 숨기는 기능은 아니다.

### 6. 면접 전에 스스로 다시 설명해 볼 질문

- `202`를 받은 뒤 조회 값이 그대로여도 정상일 수 있는 이유는 무엇인가?
- 같은 구매를 재시도하면서 새 UUID를 만들면 왜 중복 방지가 안 되는가?
- ID 저장은 성공하고 매출 갱신이 실패하면 다음 재시도는 어떻게 되는가?
- 서로 다른 이벤트가 같은 지역을 동시에 갱신하면 무엇이 증가분을 지키는가?
- 같은 ID에 다른 금액을 보내는 충돌도 현재 구현이 찾아내는가?
- 단위 테스트 통과와 실제 Kafka·PostgreSQL 연동 성공은 어떻게 다른가?

암기한 용어보다 파일을 열어 처리 흐름과 보장 범위를 설명할 수 있는지 확인한다.

### 7. 직접 실행하면서 확인하는 순서

준비물은 Java 17 이상, Maven, Python 3, 실행 중인 Docker 엔진이다. 명령은 저장소 최상위 폴더를 기준으로 적었다. 테스트는 실험용 ID와 지역을 새로 만들며, 기존 데이터를 지우지 않는다. 실습 데이터는 DB에 남으므로 운영 DB를 연결해서 실행하지 않는다.

먼저 기반 시설을 시작한다. `--wait`는 프로세스가 생성된 것뿐 아니라 상태 검사에 성공할 때까지 기다리는 옵션이다.

```sh
docker compose up -d --wait --wait-timeout 120
```

빠른 단위 테스트와 패키징은 다음과 같다. 여기까지 성공해도 실제 Kafka·DB 연결 성공을 의미하지는 않는다.

```sh
mvn -f service/pom.xml verify
```

실제 기반 시설을 사용하는 통합 테스트는 별도로 실행한다. 이 테스트가 Java 서비스를 임의의 빈 포트로 시작하므로 직접 서버를 먼저 켤 필요는 없다. Python 중복 실험 스크립트도 이 서버를 대상으로 실행한다.

```sh
mvn -f service/pom.xml -Pintegration verify
```

직접 API를 호출해 보고 싶으면 서버를 시작한다.

```sh
mvn -f service/pom.xml spring-boot:run
```

다른 터미널에서 실행한다.

```sh
python experiments/replay_duplicate.py --repeat 10
```

성공 시 `요청 10회 → 집계 1건, 금액 129000.00`이 표시된다. 기본으로 최초 반영 후 3초 더 관찰한다. `--timeout`과 `--observe-seconds`로 대기·관찰 시간을 조절할 수 있다. 실패 시 종료 코드가 1이므로 자동화에서도 실패를 구분할 수 있다.

DB 처리 기록은 스크립트가 출력한 UUID로 조회한다. 아래 `<출력된 UUID>`를 실제 값으로 바꿔 실행한다.

```sh
docker compose exec -T postgres psql -U commerce -d commerce_events -c "SELECT count(*) FROM processed_events WHERE event_id = '<출력된 UUID>';"
docker compose exec -T postgres psql -U commerce -d commerce_events -c "SELECT region, order_count, total_amount FROM regional_sales WHERE region = '중복실험-<출력된 UUID>';"
```

기대값은 처리 기록 1행, 집계 건수 1, 금액 129000.00이다. 소비자 오류는 Java 서버를 실행한 터미널에서 확인한다. Java는 컨테이너 밖에서 실행하므로 `docker compose logs`만 봐서는 Java 소비자 예외를 볼 수 없다.

공부가 끝나면 Java 서버를 종료하고 `docker compose stop`으로 기반 시설을 멈춘다. 컨테이너 삭제·재생성과 중지는 다르므로 데이터를 계속 쓸 때는 구분한다.

### 8. README와 구현을 대조한 기록

2026-09-17 기준 로컬 검증 환경은 Windows, JDK 25.0.3, Maven 3.9.15다. 컴파일 대상은 Java 17이며 GitHub CI에서는 Ubuntu와 Java 17.0.20.1로 검증했다. 실제 실행하지 않은 내용을 성공으로 기록하지 않는다.

검증한 코드 커밋은 [`78c1cfa`](https://github.com/kangwoul2/commerce-event-pipeline/commit/78c1cfa19070094ea0562ce304c71bf39b3f3853)다. [GitHub Actions 실행 결과](https://github.com/kangwoul2/commerce-event-pipeline/actions/runs/35161333243)에서 **단위 테스트 20개 + 통합 테스트 6개, 실패 0·건너뜀 0**을 확인했다. 실행 페이지의 `test-reports` 첨부 결과에는 Maven 테스트 보고서가 보관된다.

| 확인 대상 | 발견한 점과 보완 | 검증 범위 |
|---|---|---|
| Kafka 발행 후 `202` | 기존에는 비동기 발행 결과를 무시함 → 성공 확인 후 응답, 실패·시간 초과는 `503` | 발행기·모의 HTTP 테스트 |
| Spring Boot 4 자동 설정 | Kafka·Flyway 일반 라이브러리를 자동 설정 포함 스타터로 교체 | CI에서 서비스 시작, Flyway V1 적용, Kafka 발행·소비 확인 |
| Kafka JSON 변환 | 기존 Jackson 2용 변환기와 현재 Jackson 3 의존성이 맞지 않음 → 현재 버전용 변환기로 수정 | 설정 파일의 실제 클래스 생성과 이벤트 왕복 변환 검사 |
| 입력과 DB 규칙 | 지역 최대 120자, 금액 정수 18자리·소수 2자리 검증 추가 | 잘못된 입력의 `400`과 발행 미호출 검사 |
| 소비 오류 | DLQ 없이 기본 오류 처리에 의존함 → 소비 중단 정책 명시 | 코드·설정 확인, 장애 후 재시작 실험은 미실시 |
| 중복 전송 실험 | 요청만 보내던 스크립트에 집계 대기·결과 검사 추가 | Python 문법 검사와 CI에서 실제 서버 대상 스크립트 실행 성공 |
| 테스트 | 중복 분기 1개에서 발행·HTTP·변환·집계 분기 20개로 확장 | 로컬 `mvn verify` 성공, 실패 0·건너뜀 0 |
| DB 정합성과 전체 연결 | 실제 기반 시설을 쓰는 통합 테스트 6개 추가 | CI에서 중복·롤백 후 재시도·동시 처리·HTTP 전체 경로·실험 스크립트 모두 통과 |
| Compose | 서비스별 상태 검사 추가 | 로컬 설정 검사 통과, CI에서 PostgreSQL·Kafka 시작 및 상태 검사 통과 |
| GitHub CI | 단위 테스트에 더해 실제 기반 시설과 통합 테스트 실행, 결과 보고서 보관 | 해당 코드 커밋의 전체 작업 성공 및 보고서 업로드 확인 |

로컬에서는 Docker 명령 자체는 설치돼 있지만 `dockerDesktopLinuxEngine` 파이프에 연결하지 못했다. 실제 연동 검증은 GitHub CI 환경에서 완료했다. 따라서 서버 코드와 Compose의 연결은 확인했지만, 이 PC에서 서비스를 띄우려면 로컬 Docker 엔진 문제는 별도로 해결해야 한다.

통합 테스트는 같은 이벤트 24개를 동시에 처리해 1회만 반영되는지, 서로 다른 이벤트 24개가 같은 지역에 24건·246.00으로 누적되는지 확인했다. 롤백 테스트에서는 일부러 121자 지역을 보내 DB 오류를 만든 뒤 ID 기록이 남지 않는지, 같은 ID로 정상 요청을 다시 처리할 수 있는지 확인했다. 그래서 로그의 `value too long for type character varying(120)`은 이 테스트가 의도적으로 만든 오류다.

아직 남은 검증은 소비자 강제 종료 후 재전달, 브로커 장애, 잘못된 메시지 처리와 복구, 대규모 부하 실험이다. 같은 키에 다른 본문을 보내는 충돌 검사, DLQ, 소비 중단 상태 감지도 다음 개선 항목으로 남긴다.

### 9. 설명을 확인할 때 참고한 공식 문서

- [Spring Boot 4 이전 안내](https://github.com/spring-projects/spring-boot/wiki/Spring-Boot-4.0-Migration-Guide): 기능별 자동 설정과 스타터 구성을 확인했다.
- [Spring Kafka 메시지 발행](https://docs.spring.io/spring-kafka/reference/kafka/sending-messages.html): 발행 결과가 나중에 완료되며, 결과를 기다릴 때 제한 시간을 두는 방식을 확인했다.
- [Spring Kafka JSON 변환](https://docs.spring.io/spring-kafka/reference/kafka/serdes.html): 현재 버전의 JSON 변환기와 타입 정보를 전달하는 방식을 확인했다.
- [Spring Kafka 오류 처리](https://docs.spring.io/spring-kafka/reference/kafka/annotation-error-handling.html): 기본 오류 처리의 건너뛰기 동작과 소비 중단 처리기의 차이를 확인했다.
