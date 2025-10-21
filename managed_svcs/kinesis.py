from __future__ import annotations

from typing import Any, Dict, List


class KinesisStream:
    NODE_KIND = "kinesis.stream"
    IN_PORTS: List[str] = []
    OUT_PORTS: List[str] = ["records"]

    @staticmethod
    def deploy(node: Dict[str, Any], ctx: Dict[str, Any]) -> Dict[str, Any]:
        kinesis = ctx["session"].client("kinesis")
        name = node.get("props", {}).get("name", node["id"])
        shards = int(node.get("props", {}).get("shard_count", 1))
        try:
            kinesis.describe_stream_summary(StreamName=name)
        except kinesis.exceptions.ResourceNotFoundException:
            kinesis.create_stream(StreamName=name, ShardCount=shards)
            kinesis.get_waiter("stream_exists").wait(StreamName=name)
        arn = kinesis.describe_stream_summary(StreamName=name)["StreamDescriptionSummary"]["StreamARN"]
        return {"stream_name": name, "stream_arn": arn}

    @staticmethod
    def destroy(node: Dict[str, Any], ctx: Dict[str, Any]) -> None:
        kinesis = ctx["session"].client("kinesis")
        name = node.get("props", {}).get("name", node["id"])
        try:
            kinesis.delete_stream(StreamName=name, EnforceConsumerDeletion=True)
        except Exception:
            pass


SERVICE = KinesisStream
