from __future__ import annotations

import time
from typing import Any, Dict, List


class BedrockModel:
    NODE_KIND = "bedrock.model"
    IN_PORTS: List[str] = ["invoke"]
    OUT_PORTS: List[str] = ["vectors", "tokens", "invoke"]

    @staticmethod
    def _import_hf(bedrock, s3_uri: str, model_name: str, arch_hint: str | None) -> str:
        resp = bedrock.create_model_import_job(
            jobName=f"import-{model_name}",
            modelName=model_name,
            modelSource={"s3DataSource": {"s3Uri": s3_uri}},
            **({"architecture": arch_hint} if arch_hint else {}),
        )
        job_arn = resp["jobArn"]
        # naive poll
        for _ in range(120):
            d = bedrock.get_model_import_job(jobIdentifier=job_arn)
            st = d["status"]
            if st in ("Completed", "Failed"):
                if st == "Failed":
                    raise RuntimeError(f"Bedrock import failed: {d}")
                return d["modelArn"]
            time.sleep(10)
        raise TimeoutError("Model import timed out")

    @staticmethod
    def deploy(node: Dict[str, Any], ctx: Dict[str, Any]) -> Dict[str, Any]:
        br = ctx["session"].client("bedrock")
        props = node.get("props", {})
        mode = props["mode"]
        if "model_id" in props:
            # use an existing Bedrock model id
            return {"mode": mode, "model_id": props["model_id"]}
        if "import_from_s3" in props:
            arn = BedrockModel._import_hf(br, props["import_from_s3"], props["model_name"], props.get("arch_hint"))
            return {"mode": mode, "model_id": arn}
        raise ValueError("bedrock.model requires either model_id or import_from_s3")

    @staticmethod
    def wire(edge, refs, ctx) -> None:
        # IAM scoping to allow Lambda invoke handled in apigw/lambda where needed
        return


SERVICE = BedrockModel
