#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib
import json
import os
import sys
from typing import Any, Dict, List, Tuple

import boto3
import yaml

from utils.graph import validate_graph, topo_sort, port_map_from_plugins
from utils.aws import build_session, pretty_refs
from managed_svcs import REGISTRY, load_plugins


def _load_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r") as fh:
        return yaml.safe_load(fh)


def _init_session(doc: Dict[str, Any]) -> boto3.session.Session:
    region = doc.get("region") or os.environ.get("AWS_DEFAULT_REGION")
    profile = doc.get("profile") or os.environ.get("AWS_PROFILE")
    return build_session(region=region, profile=profile)


def cmd_plan(doc: Dict[str, Any]) -> None:
    port_map = port_map_from_plugins(REGISTRY)
    validate_graph(doc, port_map)
    order = topo_sort(doc["nodes"], doc["edges"])
    print("Plan OK. Deployment order:")
    for i, nid in enumerate(order, 1):
        nt = next(n["type"] for n in doc["nodes"] if n["id"] == nid)
        print(f"  {i}. {nid} ({nt})")


def cmd_deploy(doc: Dict[str, Any]) -> None:
    sess = _init_session(doc)
    port_map = port_map_from_plugins(REGISTRY)
    validate_graph(doc, port_map)
    order = topo_sort(doc["nodes"], doc["edges"])

    id2node = {n["id"]: n for n in doc["nodes"]}
    refs: Dict[str, Dict[str, Any]] = {}
    ctx = {
        "session": sess,
        "region": sess.region_name,
        "tags": doc.get("tags", {}),
        "doc": doc,
        "refs": refs,
    }

    # Deploy nodes in topological order
    for nid in order:
        node = id2node[nid]
        ntype = node["type"]
        service = REGISTRY[ntype]
        print(f"Deploying {nid} ({ntype}) ...")
        refs[nid] = service.deploy(node, ctx)

    # Wire edges after nodes exist
    for e in doc["edges"]:
        f, t, via = e["from"], e["to"], e["via"]
        ftype, ttype = id2node[f]["type"], id2node[t]["type"]
        # Allow each service to optionally handle wiring if it owns the edge
        for svc in (REGISTRY[ftype], REGISTRY[ttype]):
            if hasattr(svc, "wire"):
                svc.wire(e, refs, ctx)

    print("\n=== Deployment Outputs ===")
    print(pretty_refs(refs))


def cmd_destroy(doc: Dict[str, Any]) -> None:
    sess = _init_session(doc)
    id2node = {n["id"]: n for n in doc["nodes"]}
    # Best-effort reverse order deletion (no edge checks for brevity)
    print("Type 'destroy' to confirm teardown:", end=" ")
    if (input().strip().lower() != "destroy"):
        print("Aborted.")
        return

    for n in reversed(doc.get("nodes", [])):
        svc = REGISTRY[n["type"]]
        try:
            if hasattr(svc, "destroy"):
                print(f"Destroying {n['id']} ({n['type']}) ...")
                svc.destroy(n, {"session": sess})
        except Exception as ex:
            print(f"Warn: {n['id']}: {ex}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Composable AWS DAG deployer")
    ap.add_argument("cmd", choices=["plan", "deploy", "destroy"])
    ap.add_argument("-f", "--file", required=True, help="YAML graph file")
    args = ap.parse_args()

    load_plugins()  # auto-register managed services
    doc = _load_yaml(args.file)

    if args.cmd == "plan":
        cmd_plan(doc)
    elif args.cmd == "deploy":
        cmd_deploy(doc)
    else:
        cmd_destroy(doc)


if __name__ == "__main__":
    main()
