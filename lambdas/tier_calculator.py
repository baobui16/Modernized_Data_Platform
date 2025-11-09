"""
Lambda: Tier Calculator

Input event example:
{
  "customer_id": "cust-001",
  "monthly_spend": 1200000
}

Environment variables (optional):
- SILVER_THRESHOLD (int, default 5000000)
- GOLD_THRESHOLD (int, default 20000000)

Returns:
{ "customer_id": ..., "monthly_spend": ..., "tier": "Bronze|Silver|Gold" }
"""
import os
import json


def compute_tier(amount, silver_threshold=None, gold_threshold=None):
    if silver_threshold is None:
        silver_threshold = int(os.environ.get('SILVER_THRESHOLD', '5000000'))
    if gold_threshold is None:
        gold_threshold = int(os.environ.get('GOLD_THRESHOLD', '20000000'))

    if amount >= gold_threshold:
        return 'Gold'
    if amount >= silver_threshold:
        return 'Silver'
    return 'Bronze'


def handler(event, context=None):
    # Support both direct dict and AWS Lambda event wrapper
    body = event
    if isinstance(event, dict) and 'body' in event and isinstance(event['body'], str):
        try:
            body = json.loads(event['body'])
        except Exception:
            body = event['body']

    customer_id = body.get('customer_id')
    monthly_spend = int(body.get('monthly_spend', 0))

    tier = compute_tier(monthly_spend)

    return {
        'customer_id': customer_id,
        'monthly_spend': monthly_spend,
        'tier': tier
    }


if __name__ == '__main__':
    # quick local test
    print(handler({'customer_id': 'cust-001', 'monthly_spend': 6000000}))
