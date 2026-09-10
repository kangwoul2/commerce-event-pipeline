# Commerce Event Pipeline Interview Guide

답변 순서는 항상 `문제 → 대안 → 선택 → trade-off → 검증`을 기준으로 합니다.

## 1. 왜 Kafka를 사용했나요?

구매 API가 분석 집계까지 동기적으로 수행하면 사용자 요청 latency와 downstream workload가 결합됩니다. 또한 같은 구매 event를 분석, 추천, 알림 등 여러 consumer가 독립적으로 사용할 수 있어야 합니다. 그래서 durable log와 consumer group을 제공하는 Kafka를 선택했습니다. 단순히 대용량이라는 이유만으로 선택한 것은 아닙니다.

## 2. 왜 API가 200이 아니라 202인가요?

HTTP 응답 시점에는 event를 받아 broker에 전달하는 단계까지 수행하지만 최종 projection 완료는 consumer가 비동기로 처리합니다. 완료되지 않은 작업을 완료된 것처럼 200으로 표현하기보다 `202 Accepted`가 계약에 더 정확합니다.

## 3. Kafka ordering은 어디까지 보장되나요?

동일 partition 내부에서만 순서를 보장합니다. 전체 topic의 global ordering은 보장하지 않습니다. 이 프로젝트는 region별 projection을 사용하므로 region을 key로 보내 같은 region event가 같은 partition에 배치되도록 합니다.

## 4. region key의 단점은 무엇인가요?

traffic이 특정 region에 편중되면 hot partition이 생길 수 있습니다. 실제 분포를 측정한 뒤 `region + bucket` 같은 key를 검토할 수 있지만, 그 경우 region 단위 strict ordering이 약해지는 trade-off가 있습니다.

## 5. at-least-once가 왜 duplicate를 만들 수 있나요?

consumer가 DB effect는 완료했지만 offset commit 전에 죽으면 broker는 같은 record를 다시 전달할 수 있습니다. 그래서 consumer 호출 횟수가 아니라 최종 effect가 idempotent해야 합니다.

## 6. duplicate 처리를 어떻게 했나요?

`processed_events.event_id`에 unique/primary-key constraint를 두고 insert를 먼저 시도합니다. 새 ID인 경우에만 regional projection을 증가시킵니다. duplicate ID면 no-op합니다.

## 7. Redis distributed lock을 왜 쓰지 않았나요?

현재 문제는 여러 process가 동시에 critical section에 들어가는 것 자체보다 같은 event의 최종 효과가 두 번 적용되는 것입니다. unique constraint + transaction이 더 직접적이고 실패 복구에도 유리합니다. lock은 coordination 문제가 별도로 생겼을 때 검토합니다.

## 8. dedup insert와 aggregate update를 왜 같은 transaction으로 묶나요?

processed event만 commit된 뒤 aggregate가 실패하면 retry 시 duplicate로 판단되어 aggregate가 영원히 반영되지 않을 수 있습니다. 두 mutation을 하나의 DB transaction으로 묶어 함께 commit/rollback합니다.

## 9. Lost Update는 어떻게 막나요?

application에서 현재 합계를 읽고 더한 뒤 update하는 read-modify-write를 사용하지 않습니다. PostgreSQL atomic upsert/increment를 사용해 concurrent update를 DB가 처리하도록 합니다.

## 10. Exactly-once를 구현한 건가요?

아닙니다. end-to-end exactly-once라는 표현은 하지 않습니다. 이 프로젝트가 보장하는 것은 동일 event ID가 재전달돼도 PostgreSQL projection final effect가 중복 적용되지 않는 idempotency입니다.

## 11. Producer가 Kafka publish 전에 죽으면요?

현재 구조는 broker publish dependency를 직접 가집니다. business DB state와 event publish를 반드시 함께 보장해야 하는 서비스라면 Transactional Outbox가 필요합니다. 현재 프로젝트에는 full order transaction이 없기 때문에 Outbox를 완료 기능처럼 추가하지 않았습니다.

## 12. DLQ는 언제 필요하나요?

serialization/schema 오류처럼 retry로 해결되지 않는 poison message를 무한 반복하면 partition progress를 방해할 수 있습니다. retry limit 이후 별도 DLQ로 격리하고 원인을 관찰하는 구조가 적합합니다.

## 13. Kafka consumer lag는 왜 중요한가요?

API throughput이 정상이어도 consumer가 처리 속도를 따라가지 못하면 analytics 데이터가 오래 지연됩니다. 따라서 운영에서는 request latency와 별개로 consumer lag, processing latency, retry/error rate를 봐야 합니다.

## 14. Kafka가 과한 기술 아닌가요?

단순 CRUD 서비스라면 과할 수 있습니다. 이 프로젝트에서는 동일 event의 replay, 여러 consumer 확장, asynchronous projection을 학습하고 검증하는 것이 목적입니다. 실제 제품에서 consumer가 하나이고 volume이 작다면 DB queue 같은 더 단순한 대안을 먼저 선택할 수 있습니다.

## 15. 이 프로젝트의 가장 중요한 개념은 무엇인가요?

Kafka 자체보다 **failure를 정상 경로로 가정한 것**입니다. event가 중복될 수 있고, consumer가 중간에 죽을 수 있고, concurrent update가 발생할 수 있다고 보고 idempotency와 transaction boundary를 설계했습니다.

## 30-second answer

> 기존 E-commerce 분석 프로젝트를 실시간 구매 데이터가 생성되는 구조까지 확장했습니다. Purchase API는 이벤트를 Kafka로 발행하고 202를 반환하며, consumer는 PostgreSQL regional projection을 비동기로 갱신합니다. Kafka의 at-least-once 재전달에서 같은 구매가 두 번 집계될 수 있기 때문에 event ID를 DB unique key로 기록하고, dedup insert와 aggregate update를 하나의 transaction으로 묶었습니다. aggregate는 read-modify-write 대신 atomic SQL increment로 Lost Update를 피했습니다. Exactly-once라고 과장하지 않고, 제가 보장한 범위를 idempotent final effect로 한정해 설명합니다.
