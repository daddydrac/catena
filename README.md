# Catena
Catena — Latin for “chain,” A category-theory-inspired flow of algebras, where function composition becomes cloud composition—mathematical structure turned into deployable architecture.

# Composable AWS DAG: Algebra → Deployable Graph

This repo turns **function composition** into a **DAG of AWS managed services**.  
Think of services as morphisms and edges as composition:  
`f ∘ g ∘ h (x)` → `x -> h -> g -> f` → a **deploy order**.

## Mind Map (Algebra → Graph)

- **Objects**: data shapes at ports (e.g., `records`, `vectors`)
- **Morphisms**: node types (S3, Kinesis, Lambda, OpenSearch, Bedrock, API GW)
- **Composition**: edges with typed ports (only composable ports connect)
- **Identity**: a no-op node (optional) preserves structure

With this, your YAML is the category description; the CLI validates composition and realizes it in AWS.

## How to use

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... AWS_DEFAULT_REGION=us-east-1
# or: export AWS_PROFILE=yourprofile

python dagctl.py plan   -f graph.yaml
python dagctl.py deploy -f graph.yaml
python dagctl.py destroy -f graph.yaml
```

-------------------------

## Proj Structure

```
repo/
  README.md
  graph.yaml                  # your DAG spec
  dagctl.py                   # CLI: plan | deploy | destroy
  utils/
    aws.py                    # sessions, waiters, SigV4 auth, tagging
    graph.py                  # schema, ports, validation, topo sort
  managed_svcs/
    __init__.py               # auto-discovery registry
    base.py                   # Service interface (ports + deploy)
    s3.py                     # S3 bucket node
    kinesis.py                # Kinesis stream node
    firehose.py               # Firehose (KDS->transform->AOSS)
    lambda_fn.py              # Lambda function node
    opensearch.py             # OpenSearch Serverless vector collection+index
    bedrock.py                # Bedrock (incl. HF custom import)
    apigw.py                  # API Gateway HTTP API node
  lambda_src/
    ingester/app.py           # S3->Kinesis producer
    transform_embed/app.py    # Firehose transform: text->embedding JSON
    retriever/app.py          # /chat -> RAG (OS top-k + Bedrock chat)
```

## End-to-end System Diagram (Demo)

```
[External Source]
      |
      v
+----------------+         +---------------------+         +---------------------+
| kinesis.stream | ----->  | lambda.fn:ingester  | ----->  | opensearch.vector   |
+----------------+         +---------------------+         +---------------------+
                                 |   (calls)                        ^
                                 v                                  |
                         +------------------+                        |
                         | bedrock.model    |  (embeddings) --------+
                         +------------------+

Query path (separate DAG or shared):
+----------+     +--------------------+      +---------------------+      +------------------+
| api/http | --> | lambda.fn:retriever| -->  | opensearch.vector   | -->  | bedrock.model    |
+----------+     +--------------------+      +---------------------+      +------------------+
                                         (top-k vectors)                 (prompted answer)
```
