from __future__ import annotations
from typing import Any, Dict, List
from utils.aws import tag_list


class S3Bucket:
    NODE_KIND = "s3.bucket"
    IN_PORTS: List[str] = []
    OUT_PORTS: List[str] = ["s3_event", "s3_path"]

    @staticmethod
    def deploy(node: Dict[str, Any], ctx: Dict[str, Any]) -> Dict[str, Any]:
        s3 = ctx["session"].client("s3")
        props = node.get("props", {})
        bucket = props["bucket_name"]
        region = ctx["region"]

        exists = True
        try:
            s3.head_bucket(Bucket=bucket)
        except Exception:
            exists = False
        if not exists:
            args = {"Bucket": bucket}
            if region != "us-east-1":
                args["CreateBucketConfiguration"] = {"LocationConstraint": region}
            s3.create_bucket(**args)
            s3.put_public_access_block(
                Bucket=bucket,
                PublicAccessBlockConfiguration={
                    "BlockPublicAcls": True,
                    "IgnorePublicAcls": True,
                    "BlockPublicPolicy": True,
                    "RestrictPublicBuckets": True,
                },
            )
            s3.put_bucket_encryption(
                Bucket=bucket,
                ServerSideEncryptionConfiguration={
                    "Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]
                },
            )
        if ctx.get("tags"):
            s3.put_bucket_tagging(Bucket=bucket, Tagging={"TagSet": tag_list(ctx["tags"])})
        if props.get("lifecycle_days_glacier"):
            s3.put_bucket_lifecycle_configuration(
                Bucket=bucket,
                LifecycleConfiguration={
                    "Rules": [{
                        "ID": "to-glacier",
                        "Status": "Enabled",
                        "Transitions": [{"Days": int(props["lifecycle_days_glacier"]), "StorageClass": "GLACIER"}],
                        "Filter": {"Prefix": ""},
                    }]
                },
            )
        return {"bucket": bucket, "region": region}

    @staticmethod
    def wire(edge, refs, ctx) -> None:
        """Wire S3:ObjectCreated -> Lambda when via == 's3_event'."""
        if edge["via"] != "s3_event":
            return
        sess = ctx["session"]
        s3 = sess.client("s3")
        lam = sess.client("lambda")

        src = refs.get(edge["from"], {})
        dst = refs.get(edge["to"], {})
        bucket = src.get("bucket")
        fn_name = dst.get("function_name")
        if not bucket or not fn_name:
            return

        fn_arn = lam.get_function(FunctionName=fn_name)["Configuration"]["FunctionArn"]
        # permission
        try:
            lam.add_permission(
                FunctionName=fn_name,
                StatementId=f"s3invoke-{bucket}",
                Action="lambda:InvokeFunction",
                Principal="s3.amazonaws.com",
                SourceArn=f"arn:aws:s3:::{bucket}",
            )
        except lam.exceptions.ResourceConflictException:
            pass
        # event notification
        notif = s3.get_bucket_notification_configuration(Bucket=bucket)
        lambdas = notif.get("LambdaFunctionConfigurations", [])
        if not any(cfg.get("LambdaFunctionArn") == fn_arn for cfg in lambdas):
            lambdas.append({"LambdaFunctionArn": fn_arn, "Events": ["s3:ObjectCreated:*"]})
            s3.put_bucket_notification_configuration(
                Bucket=bucket, NotificationConfiguration={"LambdaFunctionConfigurations": lambdas}
            )

    @staticmethod
    def destroy(node: Dict[str, Any], ctx: Dict[str, Any]) -> None:
        # do not auto-empty buckets in PoC
        pass


SERVICE = S3Bucket
