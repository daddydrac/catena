from __future__ import annotations
import json

from typing import Any, Dict, List

from utils.aws import tag_list


class FirehoseDelivery:
    NODE_KIND = "firehose.delivery"
    IN_PORTS: List[str] = ["records", "transform", "destination"]
    OUT_PORTS: List[str] = ["delivery"]

    @staticmethod
    def _kds_arn(sess, name: str) -> str:
        k = sess.client("kinesis")
        return k.describe_stream_summary(StreamName=name)["StreamDescriptionSummary"]["StreamARN"]

    @staticmethod
    def _ensure_role(iam, name: str, aoss_endpoint: str) -> str:
        # Simplified trust + permissions (tighten for prod)
        assume = {
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Principal": {"Service": "firehose.amazonaws.com"}, "Action": "sts:AssumeRole"}],
        }
        try:
            arn = iam.get_role(RoleName=name)["Role"]["Arn"]
        except iam.exceptions.NoSuchEntityException:
            arn = iam.create_role(RoleName=name, AssumeRolePolicyDocument=json.dumps(assume))["Role"]["Arn"]  # type: ignore
        # Attach AWS managed policies for brevity (tighten later)
        iam.attach_role_policy(RoleName=name, PolicyArn="arn:aws:iam::aws:policy/AmazonKinesisFullAccess")
        iam.attach_role_policy(RoleName=name, PolicyArn="arn:aws:iam::aws:policy/AmazonOpenSearchServiceFullAccess")
        iam.attach_role_policy(RoleName=name, PolicyArn="arn:aws:iam::aws:policy/service-role/AWSLambdaRole")
        return arn  # noqa: E701

    @staticmethod
    def deploy(node: Dict[str, Any], ctx: Dict[str, Any]) -> Dict[str, Any]:
        import json
        fh = ctx["session"].client("firehose")
        iam = ctx["session"].client("iam")
        props = node.get("props", {})
        name = props["name"]
        stream = props["source_stream"]
        transform_lambda = props["transform_lambda"]

        # need vector store endpoint/index from refs (wired later), so create/update afterwards in wire()
        return {"delivery_name": name, "src_stream": stream, "transform_lambda": transform_lambda}

    @staticmethod
    def wire(edge, refs, ctx) -> None:
        """When we see all parts connected, create/update delivery stream."""
        import json
        # find ourselves
        for nid, r in refs.items():
            if r.get("delivery_name"):
                name = r["delivery_name"]
                stream = r["src_stream"]
                lam = r["transform_lambda"]
                # find opensearch endpoint/index
                os = next((rr for rr in refs.values() if rr.get("endpoint") and rr.get("index")), None)
                if not os:
                    continue
                sess = ctx["session"]
                fh = sess.client("firehose")
                iam = sess.client("iam")
                role_arn = FirehoseDelivery._ensure_role(iam, f"{name}-role", os["endpoint"])
                src = {
                    "KinesisStreamSourceConfiguration": {
                        "KinesisStreamARN": FirehoseDelivery._kds_arn(sess, stream),
                        "RoleARN": role_arn,
                    }
                }
                proc = {
                    "Enabled": True,
                    "Processors": [{"Type": "Lambda", "Parameters": [{"ParameterName": "LambdaArn", "ParameterValue": _lambda_arn(sess, lam)}]}],
                }
                dest = {
                    "CollectionEndpoint": os["endpoint"],
                    "IndexName": os["index"],
                    "RoleARN": role_arn,
                    "S3BackupMode": "FailedDocumentsOnly",
                }
                try:
                    fh.describe_delivery_stream(DeliveryStreamName=name)
                    v = fh.describe_delivery_stream(DeliveryStreamName=name)["DeliveryStreamDescription"]["VersionId"]
                    fh.update_destination(
                        DeliveryStreamName=name,
                        CurrentDeliveryStreamVersionId=v,
                        DestinationId="destinationId-000000000001",
                        AmazonOpenSearchServerlessDestinationUpdate=dest,
                        ProcessingConfigurationUpdate=proc,
                    )
                except fh.exceptions.ResourceNotFoundException:
                    fh.create_delivery_stream(
                        DeliveryStreamName=name,
                        DeliveryStreamType="KinesisStreamAsSource",
                        KinesisStreamSourceConfiguration=src["KinesisStreamSourceConfiguration"],
                        AmazonOpenSearchServerlessDestinationConfiguration={**dest},
                        ProcessingConfiguration=proc,
                        Tags=tag_list(ctx.get("tags", {})),
                    )
                r["endpoint"] = os["endpoint"]
                r["index"] = os["index"]

    @staticmethod
    def destroy(node: Dict[str, Any], ctx: Dict[str, Any]) -> None:
        fh = ctx["session"].client("firehose")
        name = node["props"]["name"]
        try:
            fh.delete_delivery_stream(DeliveryStreamName=name, AllowForceDelete=True)
        except Exception:
            pass


def _lambda_arn(sess, fn_name: str) -> str:
    lam = sess.client("lambda")
    return lam.get_function(FunctionName=fn_name)["Configuration"]["FunctionArn"]


SERVICE = FirehoseDelivery
