"""
Lambda: Batch Eligibility Processor

This Lambda receives a list of aggregated customer monthly spend records and:
- computes tier for each customer (reuses internal logic)
- writes/upserts results to DynamoDB table (env var DDB_TABLE)
- publishes a notification to SNS topic (env var SNS_TOPIC_ARN) for customers meeting notify threshold

Environment variables required:
- DDB_TABLE
- SNS_TOPIC_ARN
- MIN_TIER_NOTIFY (defaults to 'Silver')
"""

import os
import json
import logging
import uuid
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ddb = boto3.resource('dynamodb')
sns = boto3.client('sns')


def compute_tier(amount):
    """Determine tier based on thresholds."""
    silver_threshold = int(os.environ.get('SILVER_THRESHOLD', '5000000'))
    gold_threshold = int(os.environ.get('GOLD_THRESHOLD', '20000000'))

    if amount >= gold_threshold:
        return 'Gold'
    elif amount >= silver_threshold:
        return 'Silver'
    else:
        return 'Bronze'


def tier_rank(tier):
    """Map tier name to numeric rank."""
    ranks = {'Bronze': 1, 'Silver': 2, 'Gold': 3}
    return ranks.get(tier, 0)


def handler(event, context):
    """Main Lambda entrypoint."""
    table_name = os.environ.get('DDB_TABLE')
    sns_arn = os.environ.get('SNS_TOPIC_ARN')
    min_notify = os.environ.get('MIN_TIER_NOTIFY', 'Silver')

    if not table_name:
        raise Exception('DDB_TABLE env var is required')
    if not sns_arn:
        raise Exception('SNS_TOPIC_ARN env var is required')

    table = ddb.Table(table_name)
    records = event.get('records', []) if isinstance(event, dict) else []

    if not records:
        logger.info('No records supplied')
        return {'processed': 0}

    notify_rank = tier_rank(min_notify)
    logger.info(f"Notify threshold tier: {min_notify} (rank {notify_rank})")

    processed = 0
    with table.batch_writer() as batch:
        for r in records:
            cid = r.get('customer_id')
            spend = int(r.get('monthly_spend', 0))
            tier = compute_tier(spend)

            item = {
                'customer_id': cid,
                'transaction_id': str(uuid.uuid4()),
                'monthly_spend': spend,
                'tier': tier,
                'updated_at': int(context.get_remaining_time_in_millis() / 1000) if context else 0
            }

            batch.put_item(Item=item)
            logger.info(f"Inserted/Updated record for {cid} → Tier={tier}, Spend={spend}")

            # Publish SNS if tier >= MIN_TIER_NOTIFY
            current_rank = tier_rank(tier)
            if current_rank >= notify_rank:
                message = {
                    'customer_id': cid,
                    'tier': tier,
                    'monthly_spend': spend
                }
                try:
                    sns.publish(
                        TopicArn=sns_arn,
                        Message=json.dumps(message),
                        Subject='PromotionEligibility'
                    )
                    logger.info(f"SNS publish success for {cid} (tier={tier})")
                except ClientError as e:
                    logger.error(f"SNS publish error for {cid}: {e}")

            processed += 1

    logger.info(f"Processed {processed} records successfully")
    return {'processed': processed}
