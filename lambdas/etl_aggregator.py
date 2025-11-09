import boto3, csv, io, os
from datetime import datetime
from collections import defaultdict

s3 = boto3.client("s3")

def handler(event, context):
    bucket = os.environ["DATA_BUCKET"]
    prefix = "transactions/"
    result = defaultdict(int)

    # List all csv objects
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    for obj in resp.get("Contents", []):
        key = obj["Key"]
        if not key.endswith(".csv"):
            continue
        data = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
        reader = csv.DictReader(io.StringIO(data))
        for row in reader:
            cid = row.get("customer_id")
            amt = int(row.get("amount") or 0)
            result[cid] += amt

    # Build output records
    records = [{"customer_id": cid, "monthly_spend": amt} for cid, amt in result.items()]
    print(f"Aggregated {len(records)} customers")

    # Step Functions expects JSON serializable
    return {"records": records}
