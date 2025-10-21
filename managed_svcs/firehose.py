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
    def _ensure_role(iam, name: str) -> str:
        assume = {
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Principal": {"Service": "firehose.amazonaws.com"}, "Action": "sts:AssumeRole"}],
        }
        try:
            arn = iam.get_role(RoleName=name)["Role"]["Arn"]
        except iam.exceptions.NoSuchEntityException:
            arn = iam.create_role(RoleName=name, AssumeRolePolicyDocument=json.dumps(assume))["Role"]["Arn"]
        # PoC policies (tighten in prod)
        for pol in [
            "arn:aws:iam::aws:policy/AmazonKinesisFullAccess",
            "arn:aws:iam::aws:policy/AmazonOpenSearchServiceFullAccess",
            "arn:aws:iam::aws:policy/service-role/AWSLambdaRole",
        ]:
            try:
                iam.attach_role_policy(RoleName=name, PolicyArn=pol)
            except Exception:
                pass
        return arn

    @staticmethod
    def deploy(node: Dict[str, Any], ctx: Dict[str, Any]) -> Dict[str, Any]:
        props = node.get("props", {})
        return {
            "delivery_name": props["name"],
            "src_stream": props["source_stream"],
            "transform_lambda": props["transform_lambda"],
        }

    @staticmethod
    def wire(edge, refs, ctx) -> None:
        # Only proceed when we see all parts (delivery + vector store)
        myself = next((r for r in refs.values() if r.get("delivery_name")), None)
        vector = next((r for r in refs.values() if r.get("endpoint") and r.get("index")), None)
        if not (myself and vector):
            return

        name = myself["delivery_name"]
        stream = myself["src_stream"]
        lam_name = myself["transform_lambda"]
        os_endpoint = vector["endpoint"]
        os_index = vector["index"]

        sess = ctx["session"]
        fh = sess.client("firehose")
        iam = sess.client("iam")
        L = sess.client("lambda")

        role_arn = FirehoseDelivery._ensure_role(iam, f"{name}-role")

        # Allow Firehose to invoke the transform Lambda
        try:
            L.add_permission(
                FunctionName=_lambda_arn(sess, lam_name),
                StatementId=f"firehose-{name}",
                Action="lambda:InvokeFunction",
                Principal="firehose.amazonaws.com",
            )
        except L.exceptions.ResourceConflictException:
            pass

        src = {
            "KinesisStreamSourceConfiguration": {
                "KinesisStreamARN": FirehoseDelivery._kds_arn(sess, stream),
                "RoleARN": role_arn,
            }
        }
        proc = {
            "Enabled": True,
            "Processors": [{"Type": "Lambda", "Parameters": [{"ParameterName": "LambdaArn", "ParameterValue": _lambda_arn(sess, lam_name)}]}],
        }
        dest = {
            "CollectionEndpoint": os_endpoint,
            "IndexName": os_index,
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

        myself["endpoint"] = os_endpoint
        myself["index"] = os_index

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
