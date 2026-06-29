"""Amazon Titan text-embeddings helper for the RAG ingestion pipeline.

Mirrors stagecraft-api/app/services/embeddings.py. Uses titan-embed-text-v2
(1024-dim, normalized) via the same cross-account Bedrock path as the agents.
"""
import json

import boto3

from app.core.config import settings
from app.services.bedrock_client import _bedrock_boto3_kwargs

EMBED_MODEL_ID = "amazon.titan-embed-text-v2:0"
EMBED_DIM = 1024


def embed_text(text: str) -> list[float]:
    """Return a 1024-dim normalized embedding for the given text."""
    client = boto3.client(
        "bedrock-runtime",
        region_name=settings.AWS_REGION,
        **_bedrock_boto3_kwargs(),
    )
    body = json.dumps({"inputText": text[:8000], "dimensions": EMBED_DIM, "normalize": True})
    resp = client.invoke_model(modelId=EMBED_MODEL_ID, body=body)
    payload = json.loads(resp["body"].read())
    return payload["embedding"]


def to_pgvector(embedding: list[float]) -> str:
    """Serialize an embedding to the pgvector literal form: '[0.1,0.2,...]'."""
    return "[" + ",".join(repr(float(x)) for x in embedding) + "]"
