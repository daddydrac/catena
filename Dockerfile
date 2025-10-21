# syntax=docker/dockerfile:1.7
FROM python:3.12-slim

# Basic OS tools for an interactive CLI experience
RUN apt-get update \
  && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
     ca-certificates curl tini bash jq less nano procps \
  && rm -rf /var/lib/apt/lists/*

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Python deps
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Project files
COPY dagctl.py /app/dagctl.py
COPY utils/ /app/utils/
COPY managed_svcs/ /app/managed_svcs/
COPY lambda_src/ /app/lambda_src/
# optional default DAG (you can still bind-mount your own)
COPY graph.yaml /app/graph.yaml


# Make a convenience shim so `dagctl` is in PATH
RUN printf '#!/bin/bash\nexec python /app/dagctl.py "$@"\n' > /usr/local/bin/dagctl \
  && chmod +x /usr/local/bin/dagctl

# Default to an interactive shell; tini handles signals nicely.
ENTRYPOINT ["/usr/bin/tini","--"]
CMD ["bash","-l"]
