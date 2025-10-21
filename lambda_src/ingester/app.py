import json
import os
import boto3

s3 = boto3.client("s3")
kinesis = boto3.client("kinesis")
STREAM = os.getenv("STREAM", "rag-ingest")


def handler(event, _):
    """S3 event -> push small JSON records to Kinesis."""
    records = []
    for rec in event.get("Records", []):
        b = rec["s3"]["bucket"]["name"]
        k = rec["s3"]["object"]["key"]
        obj = s3.get_object(Bucket=b, Key=k)
        text = obj["Body"].read().decode("utf-8", errors="ignore")
        payload = {"id": k, "text": text}
        kinesis.put_record(StreamName=STREAM, Data=json.dumps(payload).encode("utf-8"), PartitionKey=k)
        records.append(k)
    return {"statusCode": 200, "count": len(records)}
