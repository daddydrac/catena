from __future__ import annotations

import io
import json
import os
import zipfile
from typing import Any, Dict, List, Optional

import boto3
from aws_requests_auth.aws_auth import AWSRequestsAuth


def build_session(region: Optional[str], profile: Optional[str]) -> boto3.session.Session:
    """Create a boto3 Session from environment/profile, preferring explicit args."""
    if profile:
        return boto3.session.Session(profile_name=profile, region_name=region)
    return boto3.session.Session(region_name=region)


def tag_list(tags: Dict[str, str]) -> List[Dict[str, str]]:
    """Convert dict to AWS Tag list."""
    return [{"Key": k, "Value": v} for k, v in tags.items()]


def make_inline_zip_from_dir(path: str) -> bytes:
    """Zip a directory for inline Lambda upload."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(path):
            for f in files:
                fp = os.path.join(root, f)
                arc = os.path.relpath(fp, start=path)
                zf.write(fp, arc)
    return buf.getvalue()


def sigv4_auth(sess: boto3.session.Session, host: str, service: str):
    """Build SigV4 auth for requests (AOSS/OpenSearch Serverless)."""
    creds = sess.get_credentials().get_frozen_credentials()
    return AWSRequestsAuth(
        aws_access_key=creds.access_key,
        aws_secret_access_key=creds.secret_key,
        aws_token=creds.token,
        aws_host=host,
        aws_region=sess.region_name,
        aws_service=service,
    )


def pretty_refs(refs: Dict[str, Dict[str, Any]]) -> str:
    """Pretty print outputs after deploy."""
    return json.dumps(refs, indent=2, sort_keys=True)
