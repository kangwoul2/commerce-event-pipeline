"""Replay one purchase request with the same Idempotency-Key.

Run the service first, then execute this script. The consumer-side processed_events
constraint should keep the regional projection from incrementing twice.
"""

import json
import uuid
from urllib.request import Request, urlopen

key = str(uuid.uuid4())
body = json.dumps({
    "customerId": "customer-1",
    "category": "Outerwear",
    "region": "Seoul",
    "amount": 129000,
}).encode()

for _ in range(2):
    request = Request(
        "http://localhost:8080/api/v1/purchases",
        data=body,
        headers={"Content-Type": "application/json", "Idempotency-Key": key},
        method="POST",
    )
    with urlopen(request) as response:
        print(response.status, response.read().decode())
