from __future__ import annotations
import json
import requests
from typing import Any, Dict, List
from utils.aws import sigv4_auth


class OpenSearchVector:
    NODE_KIND = "opensearch.vector"
    IN_PORTS: List[str] = ["vectors", "destination", "search"]
    OUT_PORTS: List[str] = ["topk"]

    @staticmethod
    def _ensure_policies(ctx, collection_name: str) -> None:
        """Create encryption, network, and data access policies (idempotent)."""
        oss = ctx["session"].client("opensearchserverless")
        acct = ctx["session"].client("sts").get_caller_identity()["Account"]

        # encryption
        try:
            oss.create_security_policy(
                type="encryption",
                name=f"{collection_name}-enc",
                policy=json.dumps({"Rules": [{"ResourceType": "collection", "Resource": [f"collection/{collection_name}"]}]}),
            )
        except oss.exceptions.ConflictException:
            pass

        # network (PoC public; swap to VPC policy in prod)
        try:
            oss.create_security_policy(
                type="network",
                name=f"{collection_name}-net",
                policy=json.dumps([{
                    "Description": "Public access for PoC",
                    "Rules": [{"ResourceType": "collection", "Resource": [f"collection/{collection_name}"]}],
                    "AllowFromPublic": True,
                }]),
            )
        except oss.exceptions.ConflictException:
            pass

        # data access: allow Firehose role + Lambda exec roles
        principals: List[str] = []
        for r in ctx["refs"].values():
            if "delivery_name" in r:
                principals.append(f"arn:aws:iam::{acct}:role/{r['delivery_name']}-role")
            if "function_name" in r:
                principals.append(f"arn:aws:iam::{acct}:role/{r['function_name']}-exec")
        if not principals:
            # create a permissive policy first time; can update later when principals exist
            principals = [f"arn:aws:iam::{acct}:root"]

        try:
            oss.create_access_policy(
                type="data",
                name=f"{collection_name}-access",
                policy=json.dumps([{
                    "Description": "PoC data access",
                    "Rules": [{
                        "Resource": [f"collection/{collection_name}", "index/*/*"],
                        "Permission": ["aoss:ReadDocument", "aoss:WriteDocument", "aoss:DescribeCollectionItems"],
                    }],
                    "Principal": principals,
                }]),
            )
        except oss.exceptions.ConflictException:
            pass

    @staticmethod
    def deploy(node: Dict[str, Any], ctx: Dict[str, Any]) -> Dict[str, Any]:
        oss = ctx["session"].client("opensearchserverless")
        props = node.get("props", {})
        cn, idx, dims = props["collection_name"], props["index_name"], int(props["dims"])

        # collection
        items = oss.list_collections(collectionFilters={"name": cn}).get("collectionSummaries", [])
        if items:
            cid = items[0]["id"]
        else:
            cid = oss.create_collection(name=cn, type="SEARCH")["id"]
        endpoint = oss.batch_get_collection(identifiers=[cid])["collectionDetails"][0]["collectionEndpoint"]

        # policies (safe to call here; repeated later in wire() to capture new principals)
        OpenSearchVector._ensure_policies(ctx, cn)

        # index
        host = endpoint.replace("https://", "")
        auth = sigv4_auth(ctx["session"], host, "aoss")
        body = {
            "settings": {"index.knn": True, "index": {"number_of_shards": 1, "number_of_replicas": 1}},
            "mappings": {
                "properties": {
                    "id": {"type": "keyword"},
                    "text": {"type": "text"},
                    "embedding": {"type": "knn_vector", "dimension": dims},
                }
            },
        }
        r = requests.put(f"{endpoint}/{idx}", auth=auth, json=body)
        if r.status_code not in (200, 201, 400):
            r.raise_for_status()
        return {"endpoint": endpoint, "index": idx, "dims": dims, "collection": cn}

    @staticmethod
    def wire(edge, refs, ctx) -> None:
        """Re-ensure data access policies after all refs (roles) exist."""
        # Run once per deploy (cheap + idempotent)
        done_flag = "__aoss_policies_done__"
        any_aoss = next((r for r in refs.values() if r.get("collection")), None)
        if not any_aoss or refs.get(done_flag):
            return
        cn = any_aoss["collection"]
        OpenSearchVector._ensure_policies(ctx, cn)
        refs[done_flag] = {"ok": True}

    @staticmethod
    def destroy(node: Dict[str, Any], ctx: Dict[str, Any]) -> None:
        oss = ctx["session"].client("opensearchserverless")
        cn = node["props"]["collection_name"]
        try:
            items = oss.list_collections(collectionFilters={"name": cn}).get("collectionSummaries", [])
            for it in items:
                oss.delete_collection(id=it["id"])
        except Exception:
            pass


SERVICE = OpenSearchVector
