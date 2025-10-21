from __future__ import annotations

from typing import Any, Dict, List

from utils.aws import tag_list


class S3Bucket:
    NODE_KIND = "s3.bucket"
    IN_PORTS: List[str] = []
    OUT_PORTS: List[str] = ["s3_event", "s3_path"]

    @staticmethod
    def deploy(node: Dict[str, Any], ctx: Dict[str, Any]) -> Dict[str, Any]:
        """Create bucket with encryption and public block."""
        s3 = ctx["session"].client("s3")
        props = node.get("props", {})
        bucket = props["bucket_name"]
        region = ctx["region"]

        # Create if not exists
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
                    "Rules": [
                        {
                            "ID": "to-glacier",
                            "Status": "Enabled",
                            "Transitions": [
                                {"Days": int(props["lifecycle_days_glacier"]), "StorageClass": "GLACIER"}
                            ],
                            "Filter": {"Prefix": ""},
                        }
                    ]
                },
            )
        return {"bucket": bucket, "region": region}

    @staticmethod
    def wire(edge, refs, ctx) -> None:
        # S3 wiring handled by Lambda (target) and S3 (source) in lambda_fn.SERVICE.wire
        return

    @staticmethod
    def destroy(node: Dict[str, Any], ctx: Dict[str, Any]) -> None:
        # Deliberately safe: we do not auto-empty buckets in PoC.
        pass


SERVICE = S3Bucket
