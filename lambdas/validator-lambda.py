import json
import boto3
import base64
import traceback

lambda_client = boto3.client('lambda')

def lambda_handler(event, context):
    print("🔥 Lambda STARTED")
    print("Raw event dump (first 500 chars):", str(event)[:500])
    try:
        records = event.get('Records', [])
        print(f"Total records received: {len(records)}")

        valid_records = []
        for record in records:
            payload_b64 = record['kinesis']['data']
            data_json = json.loads(base64.b64decode(payload_b64).decode('utf-8'))
            print("Decoded record:", data_json)

            required_fields = ['transaction_id', 'customer_id', 'amount', 'timestamp']
            if all(field in data_json for field in required_fields):
                valid_records.append(data_json)
            else:
                print("❌ Missing required fields:", data_json)

        print(f"✅ Validated {len(valid_records)} valid records")

        if valid_records:
            response = lambda_client.invoke(
                FunctionName='rules-engine-lambda',
                InvocationType='Event',
                Payload=json.dumps({'valid_records': valid_records})
            )
            print("🚀 Invoked rules-engine-lambda:", response['StatusCode'])
        else:
            print("⚠️ No valid records to process")

    except Exception as e:
        print("❌ Exception occurred:")
        traceback.print_exc()

    print("🏁 Lambda END")
    return {"status": "done"}
