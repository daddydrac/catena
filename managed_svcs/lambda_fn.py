from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from utils.aws import make_inline_zip_from_dir


class LambdaFn:
    NODE_KIND = "lambda.fn"
    IN_PORTS: List[str] = ["records", "s3_event", "http", "invoke"]
    OUT_PORTS: List[str] = ["invoke", "s3_put", "vectors"]

    @staticmethod
    def _ensure_role(iam, role_name: str) -> str:
        assume = {
            "Version": "2012-10-17",
            "Statement": [
                {"Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}
            ],
        }
        try:
            return iam.get_role(RoleName=role_name)["Role"]["Arn"]
        except iam.exceptions.NoSuchEntityException:
            arn = iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=json.dumps(assume))["Role"]["Arn"]
            iam.attach_role_policy(
                RoleName=role_name, PolicyArn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
            )
            return arn

    @staticmethod
    def deploy(node: Dict[str, Any], ctx: Dict[str, Any]) -> Dict[str, Any]:
        props = node.get("props", {})
        lam = ctx["session"].client("lambda")
        iam = ctx["session"].client("iam")

        fn = props["function_name"]
        role_arn = LambdaFn._ensure_role(iam, f"{fn}-exec")
        code_zip = make_inline_zip_from_dir(props["source_dir"]) if props.get("source_dir") else None
        if not code_zip:
            # trivial hello
            code_zip = make_inline_zip_from_dir("lambda_src/ingester")

        create_args = {
            "FunctionName": fn,
            "Runtime": props["runtime"],
            "Role": role_arn,
            "Handler": props.get("handler", "app.handler"),
            "Code": {"ZipFile": code_zip},
            "Timeout": int(props["timeout_s"]),
            "MemorySize": int(props["memory_mb"]),
            "Tags": ctx.get("tags", {}),
            "Environment": {"Variables": props.get("env", {})},
        }
        try:
            lam.get_function(FunctionName=fn)
            lam.update_function_code(FunctionName=fn, ZipFile=code_zip, Publish=True)
            lam.update_function_configuration(
                FunctionName=fn,
                Role=role_arn,
                Runtime=props["runtime"],
                Handler=create_args["Handler"],
                Timeout=int(props["timeout_s"]),
                MemorySize=int(props["memory_mb"]),
                Environment=create_args["Environment"],
            )
        except lam.exceptions.ResourceNotFoundException:
            lam.create_function(**create_args)

        arn = lam.get_function(FunctionName=fn)["Configuration"]["FunctionArn"]
        return {"function_name": fn, "lambda_arn": arn}

    @staticmethod
    def wire(edge, refs, ctx) -> None:
        """S3->Lambda notifications and API GW->Lambda integrations are applied in target services."""
        return

    @staticmethod
    def destroy(node: Dict[str, Any], ctx: Dict[str, Any]) -> None:
        lam = ctx["session"].client("lambda")
        fn = node["props"]["function_name"]
        try:
            lam.delete_function(FunctionName=fn)
        except Exception:
            pass


SERVICE = LambdaFn
