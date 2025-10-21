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
docker build -t catena:latest .

# Use it like a CLI (interactive)
# Shell in with your AWS creds mounted or env’d

docker run --rm -it \
  -e AWS_PROFILE=default \
  -e AWS_DEFAULT_REGION=us-east-1 \
  -v $HOME/.aws:/home/app/.aws:ro \
  -v $(pwd)/graph.yaml:/app/graph.yaml:ro \
  catena:latest

# One-off command (non-interactive)

docker run --rm \
  -e AWS_PROFILE=default \
  -e AWS_DEFAULT_REGION=us-east-1 \
  -v $HOME/.aws:/home/app/.aws:ro \
  -v $(pwd)/graph.yaml:/app/graph.yaml:ro \
  catena:latest dagctl plan -f /app/graph.yaml

# “Run infinity” then bash in later
# Start and keep it alive

docker run -d --name catena \
  -e AWS_PROFILE=default \
  -e AWS_DEFAULT_REGION=us-east-1 \
  -v $HOME/.aws:/home/app/.aws:ro \
  -v $(pwd)/graph.yaml:/app/graph.yaml:ro \
  catena:latest sleep infinity

# Enter when you want
docker exec -it catena bash -l

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

## How to add more managed services (Neptune, DynamoDB, etc.)

Create managed_svcs/<service>.py:
```
from typing import Any, Dict, List

class NeptuneCluster:
    NODE_KIND = "neptune.cluster"
    IN_PORTS: List[str] = ["gremlin", "sparql"]
    OUT_PORTS: List[str] = ["endpoint"]

    @staticmethod
    def deploy(node: Dict[str, Any], ctx: Dict[str, Any]) -> Dict[str, Any]:
        # boto3 deploy code here...
        return {"endpoint": "https://..."}

    @staticmethod
    def wire(edge, refs, ctx) -> None:
        # optional IAM/event wiring
        return

SERVICE = NeptuneCluster
```

Reference type: `neptune.cluster` in graph.yaml.

The CLI will auto-register it next run.

Complete, runnable scaffold, to stand up a secure PoC fast while keeping the design mathematically composable and extensible as your graph grows.

