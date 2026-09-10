# Commerce Event Pipeline Architecture Decisions

## ADR-001. Purchase API와 analytics projection 분리

### Context
구매 요청 처리와 지역별 통계 집계를 같은 HTTP transaction에서 처리하면 downstream 분석 작업이 사용자 요청 latency에 직접 결합됩니다.

### Decision
구매 API는 event를 Kafka에 발행하고 `202 Accepted`를 반환합니다. 지역별 매출 projection은 consumer가 비동기로 갱신합니다.

### Trade-off
- 장점: ingestion latency와 projection workload 분리
- 장점: 동일 event를 여러 consumer가 독립적으로 사용할 수 있음
- 단점: eventual consistency 발생
- 단점: broker availability와 consumer lag를 운영해야 함

---

## ADR-002. Kafka message key로 region 사용

### Context
Kafka는 partition 내부 ordering을 보장합니다. 주요 projection 단위가 region이므로 같은 region event를 같은 partition으로 보내는 것이 직관적입니다.

### Decision
`region`을 record key로 사용합니다.

### Risk
특정 region traffic이 압도적으로 많으면 hot partition이 될 수 있습니다. 실제 traffic skew를 측정한 뒤 key 재설계를 검토해야 합니다.

---

## ADR-003. Idempotent consumer를 DB unique key로 구현

### Context
at-least-once consumer는 같은 event를 재수신할 수 있습니다. process memory set은 multi-instance와 restart에 안전하지 않습니다.

### Decision
`processed_events.event_id`를 PostgreSQL PK/unique boundary로 사용합니다.

### Why not Redis lock?
이 문제는 critical section mutual exclusion보다 duplicate final effect 방지가 핵심입니다. DB unique constraint가 더 작고 직접적인 해결책입니다.

---

## ADR-004. Dedup과 projection update를 같은 transaction으로 묶음

### Context
processed event 기록만 commit되고 aggregate update가 실패하면 retry 시 duplicate로 인식되어 projection이 영구적으로 누락될 수 있습니다.

### Decision
`processed_events insert + regional_sales update`를 하나의 local DB transaction으로 처리합니다.

---

## ADR-005. Regional aggregate는 atomic SQL update

### Context
`SELECT → application increment → UPDATE`는 concurrent consumer에서 Lost Update를 만들 수 있습니다.

### Decision
PostgreSQL upsert/increment statement로 shared counter를 DB에서 atomic하게 변경합니다.

---

## ADR-006. Exactly-once라는 표현을 사용하지 않음

현재 설계는 end-to-end exactly-once를 보장한다고 주장하지 않습니다. 보장 범위는 **동일 event ID 재전달 시 PostgreSQL projection final effect를 중복 적용하지 않는 것**입니다.

---

## ADR-007. Outbox는 현재 미구현

Producer-side DB business transaction과 Kafka publish를 원자적으로 연결해야 하는 요구가 생기면 Transactional Outbox를 검토합니다. 현재 저장소는 complete commerce transaction system이 아니므로 Outbox를 구현 완료 기능처럼 표시하지 않습니다.

---

## ADR-008. DLQ는 poison message 요구가 생길 때 적용

모든 오류를 무한 retry하지 않습니다. schema/business validation처럼 retry로 해결되지 않는 오류는 제한된 재시도 후 DLQ로 격리하는 것이 다음 단계입니다.
