<div align="center">

# 🛒 E-commerce SQL & Excel Analytics

### 구매 데이터와 지역별 기온을 결합한 판매 전략 분석

![SQL](https://img.shields.io/badge/SQL-Data%20Aggregation-4479A1?style=flat-square&logo=mysql&logoColor=white)
![Excel](https://img.shields.io/badge/Excel-Analysis%20%26%20Visualization-217346?style=flat-square&logo=microsoftexcel&logoColor=white)
![Data](https://img.shields.io/badge/Data-E--commerce-6C63FF?style=flat-square)

**Role · 한영서 — 지역/기온 기반 아우터 판매 전략 분석 · 시각화 · 결과 보고 · 발표**

</div>

---

## 1. Project Overview

전자상거래 구매 데이터만 보는 대신 **고객의 지역 정보와 지역별 평균 기온 데이터를 결합**해 상품 구매 패턴을 분석한 프로젝트입니다.

분석 목표는 단순 EDA가 아니라 다음 질문에 답하는 것이었습니다.

> **“어떤 고객군과 지역에 어떤 상품을 집중해야 단기 판매 전략을 더 구체적으로 만들 수 있을까?”**

팀은 구매 행동, 구독 상태, 고객 충성도, 색상, 지역 등 여러 관점에서 데이터를 분석했고, 저는 **지역·기온과 아우터 구매의 관계를 중심으로 판매 전략을 도출**했습니다.

---

## 2. Data Flow

```text
E-commerce Transaction Data
          │
          ├───────────────┐
          │               │
          ▼               ▼
 Customer / Product    Location
                          │
                          ▼
                Regional Temperature Data
                          │
                          ▼
                     Data Join
                          │
                          ▼
                 SQL Aggregation
                          │
                          ▼
                Excel Analysis / Chart
                          │
                          ▼
                   Business Insight
```

### Dataset

- E-commerce transaction data: 고객·구매·상품·지역·결제·구독 관련 변수
- US regional temperature data: 지역별 월/연 평균 기온

두 데이터셋을 `Location` 기준으로 연결해 구매 행동을 외부 환경 요인과 함께 분석했습니다.

---

## 3. My Contribution

### 지역·기온 기반 아우터 판매 전략

제가 담당한 분석에서는 다음 흐름을 사용했습니다.

1. 고객 거래 데이터에서 지역·상품 카테고리·구매금액 추출
2. 지역별 기온 데이터를 결합
3. 지역/기온 구간별 아우터 구매 패턴 비교
4. 단순 상관관계가 아닌 판매 전략 관점에서 결과 해석
5. Excel 기반 시각화 및 발표 자료 구성

단순히 `GROUP BY` 결과를 나열하기보다 **어떤 지역을 우선적으로 볼 것인지, 어떤 변수가 판매 전략과 연결되는지**를 가설로 먼저 설정하고 분석했습니다.

---

## 4. Data Modeling & Transformation

원본 데이터의 범주형 변수를 분석 목적에 맞게 변환하고, 기존 변수로부터 파생 지표를 만들었습니다.

예시:

```text
Location
→ 기온 데이터와 결합

Frequency of Purchases
→ 연간 구매 빈도로 변환

Previous Purchases + Purchase Frequency
→ 고객 충성도 분석용 파생 지표
```

분석 목적에 따라 원본 데이터를 그대로 사용하는 대신 **질문에 맞는 데이터셋을 다시 구성하는 과정**을 경험했습니다.

---

## 5. SQL / Excel Usage

### SQL

- `GROUP BY`
- `CASE WHEN`
- `AVG`, `SUM`, `ROUND`
- 조건 필터링
- 분석 목적별 테이블/집계 구조 생성

### Excel

- `INDEX`, `MATCH`
- `LET`
- `VSTACK`, `HSTACK`
- `RANK.EQ`
- 기초 통계 및 시각화

---

## 6. Engineering Perspective

이 프로젝트는 백엔드 프로젝트는 아니지만, 이후 서버 개발을 공부하면서 도움이 된 경험이 있습니다.

### 6.1 데이터를 저장하는 것과 질문하기 좋은 형태로 만드는 것은 다르다

원본 데이터는 다양한 변수를 포함하지만 특정 분석 질문에 바로 적합하지 않았습니다. 필요한 기준으로 데이터를 다시 변환하고 결합하면서 **사용 패턴에 맞는 데이터 모델의 중요성**을 경험했습니다.

### 6.2 JOIN key의 의미가 중요하다

서로 다른 출처의 데이터를 단순히 합치는 것이 아니라 `Location`이라는 공통 기준의 의미와 데이터 정합성을 확인해야 했습니다. 이후 RDB 설계에서 PK/FK 및 데이터 정합성을 이해하는 기초가 되었습니다.

### 6.3 결과보다 재현 가능한 과정

```text
Question
→ Hypothesis
→ Data Selection
→ Transformation
→ Aggregation
→ Visualization
→ Interpretation
```

분석을 이 순서로 구조화해 팀원과 결과를 공유했습니다.

---

## 7. Repository Contents

```text
.
├── README.md
├── e-commerce_data.xlsx       # 분석 데이터
├── add_data_analysis.xlsx     # 추가 분석/가공 결과
└── project presentation.pptx  # 최종 발표 자료
```

---

## 8. What I Learned

- 여러 출처의 데이터를 **공통 키를 기준으로 결합하는 과정**
- 분석 질문에 맞게 데이터를 재구성하는 방법
- SQL 집계 결과를 비즈니스 가설과 연결하는 방법
- 팀 분석 결과를 하나의 스토리로 문서화하고 발표하는 방법

---

## 9. Why this repository is in my backend portfolio

백엔드 개발에서도 결국 서비스가 저장하고 조회하는 것은 데이터입니다.

이 프로젝트는 제가 이후 API·DB·RAG 시스템을 공부하기 전에 **데이터를 어떤 기준으로 구조화해야 실제 질문에 답할 수 있는지**를 경험했던 프로젝트입니다.

현재는 이 경험을 바탕으로 SQL 분석을 넘어 PostgreSQL 데이터 모델링, Transaction, Index, 동시성 제어까지 학습 범위를 확장하고 있습니다.

---

<div align="center">

**From raw data to a reproducible decision process.**

</div>
