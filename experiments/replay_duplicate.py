"""같은 구매를 반복 요청하고, 실험 전용 지역의 집계가 한 번만 증가하는지 검사한다."""

import argparse
import json
import sys
import time
import uuid
from decimal import Decimal
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def read_json(url, timeout, data=None, headers=None):
    request = Request(url, data=data, headers=headers or {})
    with urlopen(request, timeout=timeout) as response:
        return response.status, json.loads(response.read().decode(), parse_float=Decimal)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:8080")
    parser.add_argument("--repeat", type=int, default=5, help="같은 요청을 보낼 횟수")
    parser.add_argument("--timeout", type=float, default=30, help="집계 대기 제한 시간(초)")
    parser.add_argument("--observe-seconds", type=float, default=3, help="반영 뒤 추가 관찰 시간(초)")
    args = parser.parse_args()
    if args.repeat < 2 or args.timeout <= 0 or args.observe_seconds <= 0:
        parser.error("반복 횟수는 2 이상, 시간은 0보다 커야 합니다.")

    key = str(uuid.uuid4())
    region = f"중복실험-{key}"
    amount = Decimal("129000.00")
    body = json.dumps({"customerId": "실험용-고객", "category": "옷", "region": region,
                       "amount": 129000}, ensure_ascii=False).encode("utf-8")
    base = args.base_url.rstrip("/")
    print(f"이벤트 ID: {key}\n실험 지역: {region}")
    for _ in range(args.repeat):
        status, result = read_json(base + "/api/v1/purchases", args.timeout, body,
                                  {"Content-Type": "application/json", "Idempotency-Key": key})
        if status != 202 or result.get("eventId") != key:
            raise RuntimeError(f"예상하지 못한 접수 응답: {status}, {result}")

    deadline = time.monotonic() + args.timeout
    observed_at = None
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise RuntimeError("제한 시간 안에 집계 1회 반영과 추가 관찰을 완료하지 못했습니다.")
        status, rows = read_json(base + "/api/v1/analytics/regions", min(5, remaining))
        if status != 200 or not isinstance(rows, list):
            raise RuntimeError("집계 조회 응답이 올바르지 않습니다.")
        row = next((item for item in rows if item["region"] == region), None)
        if row is not None:
            if row["orderCount"] != 1 or Decimal(str(row["totalAmount"])) != amount:
                raise RuntimeError(f"예상한 집계(1건, {amount})와 다릅니다: {row}")
            if observed_at is None:
                observed_at = time.monotonic()
            if time.monotonic() - observed_at >= args.observe_seconds:
                print(f"검증 성공: 요청 {args.repeat}회 → 집계 1건, 금액 {amount}")
                print("DB 처리 기록 행 수와 소비자 오류 로그는 별도로 확인해야 합니다.")
                return 0
        elif observed_at is not None:
            raise RuntimeError("한 번 조회된 실험 지역이 사라졌습니다.")
        time.sleep(min(0.25, max(0, deadline - time.monotonic())))


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (HTTPError, URLError, TimeoutError, RuntimeError, ValueError, KeyError) as error:
        print(f"검증 실패: {error}", file=sys.stderr)
        sys.exit(1)
