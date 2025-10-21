import json
import os
import boto3

bedrock = boto3.client("bedrock-runtime")
EMBED_MODEL_ID = os.getenv("EMBED_MODEL_ID", "amazon.titan-embed-text-v2:0")


def _embed(text: str):
    body = {"inputText": text}
    resp = bedrock.invoke_model(modelId=EMBED_MODEL_ID, body=json.dumps(body))
    return json.loads(resp["body"].read())["embedding"]


def handler(event, _):
    """
    Firehose Lambda Transform: receives 'records' and must return transformed batch:
    { "records": [ { "recordId":..., "result":"Ok", "data": base64(json) }, ... ] }
    Each data payload will be indexed into OpenSearch by Firehose destination.
    """
    import base64
    out = {"records": []}
    for r in event.get("records", []):
        try:
            payload = json.loads(base64.b64decode(r["data"]))
            vec = _embed(payload["text"])
            doc = {"id": payload["id"], "text": payload["text"], "embedding": vec}
            enc = base64.b64encode(json.dumps(doc).encode("utf-8")).decode("utf-8")
            out["records"].append({"recordId": r["recordId"], "result": "Ok", "data": enc})
        except Exception:
            out["records"].append({"recordId": r["recordId"], "result": "ProcessingFailed", "data": r["data"]})
    return out
