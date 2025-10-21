import json
import os
import requests
import boto3
from aws_requests_auth.aws_auth import AWSRequestsAuth

sess = boto3.session.Session()
bedrock = sess.client("bedrock-runtime")

INDEX = os.getenv("OPENSEARCH_INDEX", "docs")
COLLECTION = os.getenv("COLLECTION_NAME", "rag-vec")
EMBED_ID = os.getenv("EMBED_MODEL_ID", "amazon.titan-embed-text-v2:0")
CHAT_ID = os.getenv("CHAT_MODEL_ID")  # could be a full ARN if custom import

# Resolve AOSS endpoint by collection name each time (cache in env for perf)
def _aoss_endpoint():
    oss = sess.client("opensearchserverless")
    items = oss.list_collections(collectionFilters={"name": COLLECTION}).get("collectionSummaries", [])
    if not items:
        raise RuntimeError("OpenSearch collection not found")
    detail = oss.batch_get_collection(identifiers=[items[0]["id"]])["collectionDetails"][0]
    return detail["collectionEndpoint"]


def _auth(host):
    creds = sess.get_credentials().get_frozen_credentials()
    return AWSRequestsAuth(creds.access_key, creds.secret_key, creds.token, host, sess.region_name, "aoss")


def _embed(text: str):
    body = {"inputText": text}
    resp = bedrock.invoke_model(modelId=EMBED_ID, body=json.dumps(body))
    return json.loads(resp["body"].read())["embedding"]


def _topk(vec, k=5):
    ep = _aoss_endpoint()
    host = ep.replace("https://", "")
    q = {"size": k, "query": {"knn": {"embedding": {"vector": vec, "k": k}}}}
    r = requests.get(f"{ep}/{INDEX}/_search", auth=_auth(host), json=q, timeout=10)
    r.raise_for_status()
    return [h["_source"] for h in r.json().get("hits", {}).get("hits", [])]


def _chat(prompt: str, context_docs):
    ctx = "\n\n".join(d.get("text", "") for d in context_docs)
    body = {"inputText": f"Use the context to answer.\n\nContext:\n{ctx}\n\nQuestion:\n{prompt}", "inferenceConfig": {"temperature": 0.2}}
    resp = bedrock.invoke_model(modelId=CHAT_ID, body=json.dumps(body))
    return json.loads(resp["body"].read()).get("outputText", "")


def handler(event, _):
    body = json.loads(event.get("body", "{}"))
    q = body.get("q", "")
    vec = _embed(q)
    docs = _topk(vec, 5)
    ans = _chat(q, docs)
    return {"statusCode": 200, "headers": {"content-type": "application/json"}, "body": json.dumps({"answer": ans, "docs": docs})}
