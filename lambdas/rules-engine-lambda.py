import json
import boto3
import os

# --- AWS Clients ---
dynamodb = boto3.resource('dynamodb')
sns = boto3.client('sns')

# --- Environment variables (must exist in Lambda configuration) ---
DDB_TABLE = os.environ['DDB_TABLE']
SNS_TOPIC_ARN = os.environ['SNS_TOPIC_ARN']

def lambda_handler(event, context):
    print("Received event:", json.dumps(event))

    table = dynamodb.Table(DDB_TABLE)
    records = event.get('valid_records', [])
    print(f"Processing {len(records)} records")

    for rec in records:
        txn_id = rec['transaction_id']
        cid = rec['customer_id']
        amount = rec['amount']
        ts = rec['timestamp']
        eligible = amount >= 1_000_000

        # --- Write to DynamoDB ---
        item = {
            'transaction_id': txn_id,
            'customer_id': cid,
            'amount': amount,
            'eligible': eligible,
            'timestamp': ts
        }
        table.put_item(Item=item)
        print(f"Wrote record {txn_id} (eligible={eligible}) to DynamoDB")

        # --- Publish SNS if eligible ---
        if eligible:
            message = {
                'transaction_id': txn_id,
                'customer_id': cid,
                'amount': amount,
                'eligible': eligible,
                'timestamp': ts
            }
            sns.publish(
                TopicArn=SNS_TOPIC_ARN,
                Subject='New Eligible Transaction',
                Message=json.dumps(message, indent=2)
            )
            print(f"SNS sent for {txn_id}")

    print("Lambda completed")
    return {"status": "processed", "count": len(records)}
