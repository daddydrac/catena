from __future__ import annotations

from typing import Any, Dict, List


class ApiHttp:
    NODE_KIND = "apigw.http"
    IN_PORTS: List[str] = []
    OUT_PORTS: List[str] = ["http"]

    @staticmethod
    def deploy(node: Dict[str, Any], ctx: Dict[str, Any]) -> Dict[str, Any]:
        api = ctx["session"].client("apigatewayv2")
        name = node["props"]["name"]
        # create or get HTTP API
        apis = api.get_apis().get("Items", [])
        found = next((a for a in apis if a["Name"] == name and a["ProtocolType"] == "HTTP"), None)
        if not found:
            created = api.create_api(Name=name, ProtocolType="HTTP")
            api_id = created["ApiId"]
        else:
            api_id = found["ApiId"]

        # Default stage
        try:
            api.get_stage(ApiId=api_id, StageName="$default")
        except Exception:
            api.create_stage(ApiId=api_id, StageName="$default", AutoDeploy=True)

        url = f"https://{api_id}.execute-api.{ctx['region']}.amazonaws.com"
        return {"api_id": api_id, "invoke_url": url}

    @staticmethod
    def wire(edge, refs, ctx) -> None:
        """Connect API -> Lambda (retriever) for POST /chat."""
        if edge["via"] != "http":
            return
        # find api + lambda
        api_ref = next((r for r in refs.values() if r.get("api_id")), None)
        lam_ref = next((r for r in refs.values() if r.get("function_name") == "rag-retriever"), None)
        if not api_ref or not lam_ref:
            return
        api = ctx["session"].client("apigatewayv2")
        lam = ctx["session"].client("lambda")
        api_id = api_ref["api_id"]
        fn = lam_ref["function_name"]
        fn_arn = lam.get_function(FunctionName=fn)["Configuration"]["FunctionArn"]

        # add permission
        try:
            lam.add_permission(
                FunctionName=fn,
                StatementId=f"apigw-{api_id}",
                Action="lambda:InvokeFunction",
                Principal="apigateway.amazonaws.com",
            )
        except lam.exceptions.ResourceConflictException:
            pass

        # integration
        integ = api.create_integration(
            ApiId=api_id,
            IntegrationType="AWS_PROXY",
            IntegrationUri=f"arn:aws:apigateway:{ctx['region']}:lambda:path/2015-03-31/functions/{fn_arn}/invocations",
            PayloadFormatVersion="2.0",
        )
        # route
        api.create_route(ApiId=api_id, RouteKey="POST /chat", Target=f"integrations/{integ['IntegrationId']}")

    @staticmethod
    def destroy(node: Dict[str, Any], ctx: Dict[str, Any]) -> None:
        api = ctx["session"].client("apigatewayv2")
        name = node["props"]["name"]
        for a in api.get_apis().get("Items", []):
            if a["Name"] == name and a["ProtocolType"] == "HTTP":
                api.delete_api(ApiId=a["ApiId"])


SERVICE = ApiHttp
