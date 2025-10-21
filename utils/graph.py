from __future__ import annotations
from typing import Any, Dict, List, Set


def port_map_from_plugins(registry) -> Dict[str, Dict[str, List[str]]]:
    return {k: {"in": v.IN_PORTS, "out": v.OUT_PORTS} for k, v in registry.items()}


def validate_graph(doc: Dict[str, Any], ports: Dict[str, Dict[str, List[str]]]) -> None:
    nodes = doc.get("nodes", [])
    edges = doc.get("edges", [])
    types = {n["id"]: n["type"] for n in nodes}
    id2node = {n["id"]: n for n in nodes}

    # Node types known
    for n in nodes:
        if n["type"] not in ports:
            raise ValueError(f"Unsupported node type: {n['type']}")

    # Port checks
    for e in edges:
        f, t, via = e["from"], e["to"], e["via"]
        if types[f] not in ports or types[t] not in ports:
            raise ValueError(f"Unknown types on edge: {e}")
        if via not in ports[types[f]]["out"]:
            raise ValueError(f"Edge via '{via}' not produced by {types[f]}")
        if via not in ports[types[t]]["in"]:
            raise ValueError(f"Edge via '{via}' not accepted by {types[t]}")

    # Minimal consistency checks (PoC)
    # 1) producer STREAM set and references an existing kinesis.stream
    prod = next((n for n in nodes if n["type"] == "lambda.fn" and n["id"] == "s3_producer"), None)
    if prod:
        stream = (prod.get("props", {}).get("env") or {}).get("STREAM")
        if not stream:
            raise ValueError("s3_producer.props.env.STREAM must be set (e.g., rag-ingest)")
        kds = next((n for n in nodes if n["type"] == "kinesis.stream" and (n.get("props", {}).get("name", n["id"]) == stream)), None)
        if not kds:
            raise ValueError(f"s3_producer STREAM='{stream}' does not match any kinesis.stream name/id")

    # 2) vector dims sanity (retriever env vs vector_store props)
    vec = next((n for n in nodes if n["type"] == "opensearch.vector"), None)
    ret = next((n for n in nodes if n["id"] == "retriever" and n["type"] == "lambda.fn"), None)
    if vec and ret:
        vd = int(vec.get("props", {}).get("dims", 0))
        rd = int((ret.get("props", {}).get("env") or {}).get("DIMS", "0"))
        if vd and rd and vd != rd:
            raise ValueError(f"Dims mismatch: opensearch.vector={vd} vs retriever.env.DIMS={rd}")

    topo_sort(nodes, edges)


def topo_sort(nodes: List[Dict[str, Any]], edges: List[Dict[str, str]]) -> List[str]:
    '''kahns algo'''
    ids = [n["id"] for n in nodes]
    indeg = {i: 0 for i in ids}
    adj = {i: [] for i in ids}
    for e in edges:
        adj[e["from"]].append(e["to"])
        indeg[e["to"]] += 1
    q = [i for i in ids if indeg[i] == 0]
    order = []
    while q:
        u = q.pop(0)
        order.append(u)
        for v in adj[u]:
            indeg[v] -= 1
            if indeg[v] == 0:
                q.append(v)
    if len(order) != len(ids):
        raise ValueError("Cycle detected")
    return order
