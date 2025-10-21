from __future__ import annotations

import requests
from typing import Any, Dict, List

from utils.aws import sigv4_auth


class OpenSearchVector:
    NODE_KIND = "opensearch.vector"
    IN_PORTS: List[str] = ["vectors", "destination", "search"]
    OUT_PORTS: List[str] = ["topk"]

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
        if r.status_code not in (200, 201, 400):  # 400 if exists
            r.raise_for_status()
        return {"endpoint": endpoint, "index": idx, "dims": dims}

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
