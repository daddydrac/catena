from __future__ import annotations

from typing import Any, Dict, List, Set


def port_map_from_plugins(registry) -> Dict[str, Dict[str, List[str]]]:
    """Build a {node_type: {in:[...], out:[...]}} map from plug-ins."""
    return {k: {"in": v.IN_PORTS, "out": v.OUT_PORTS} for k, v in registry.items()}


def validate_graph(doc: Dict[str, Any], ports: Dict[str, Dict[str, List[str]]]) -> None:
    """Validate nodes, props, and port compatibility."""
    nodes = doc.get("nodes", [])
    edges = doc.get("edges", [])
    types = {n["id"]: n["type"] for n in nodes}

    # Node type known?
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

    # Topo
    topo_sort(nodes, edges)


def topo_sort(nodes: List[Dict[str, Any]], edges: List[Dict[str, str]]) -> List[str]:
    """Kahn's algorithm for DAG ordering."""
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
