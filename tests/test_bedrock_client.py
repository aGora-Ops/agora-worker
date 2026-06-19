"""Tests for BedrockRemediationClient response parsing."""
import json
from unittest.mock import MagicMock, patch
import pytest

import os
os.environ.setdefault("DATABASE_URL", "postgresql://x:x@localhost/x")
os.environ.setdefault("SECRET_KEY", "test-secret-only")

def _make_bedrock_response(content: str) -> dict:
    """Build a mock Bedrock SDK response dict with the given content text."""
    body_bytes = json.dumps({"content": [{"text": content}]}).encode()
    mock_body = MagicMock()
    mock_body.read.return_value = body_bytes
    return {"body": mock_body}

@patch("boto3.client")
def test_valid_json_response_parsed(mock_boto):
    from app.services.bedrock_client import BedrockRemediationClient

    good_response = json.dumps({
        "root_cause": "Missing env var NODE_ENV",
        "fixed_yaml": "name: CI\non: push\njobs: {}",
        "pr_title": "fix: add NODE_ENV",
        "pr_description": "Adds missing NODE_ENV variable",
    })

    mock_client = MagicMock()
    mock_client.invoke_model.return_value = _make_bedrock_response(good_response)
    mock_client.exceptions.ThrottlingException = Exception
    mock_boto.return_value = mock_client

    client = BedrockRemediationClient()
    result = client.analyze_failure("yaml", "logs", "CI", "org/repo")

    assert result["root_cause"] == "Missing env var NODE_ENV"
    assert "fixed_yaml" in result

@patch("boto3.client")
def test_markdown_fenced_json_parsed(mock_boto):
    """Bedrock sometimes wraps output in ```json ... ``` — must be stripped."""
    from app.services.bedrock_client import BedrockRemediationClient

    wrapped = "```json\n" + json.dumps({
        "root_cause": "r",
        "fixed_yaml": "y",
        "pr_title": "t",
        "pr_description": "d",
    }) + "\n```"

    mock_client = MagicMock()
    mock_client.invoke_model.return_value = _make_bedrock_response(wrapped)
    mock_client.exceptions.ThrottlingException = Exception
    mock_boto.return_value = mock_client

    client = BedrockRemediationClient()
    result = client.analyze_failure("yaml", "logs", "CI", "org/repo")
    assert result["root_cause"] == "r"

@patch("boto3.client")
def test_missing_required_key_raises(mock_boto):
    """A Bedrock response missing a required field must raise RuntimeError."""
    from app.services.bedrock_client import BedrockRemediationClient

    partial = json.dumps({"root_cause": "r", "fixed_yaml": "y"})

    mock_client = MagicMock()
    mock_client.invoke_model.return_value = _make_bedrock_response(partial)
    mock_client.exceptions.ThrottlingException = Exception
    mock_boto.return_value = mock_client

    client = BedrockRemediationClient()
    with pytest.raises(RuntimeError, match="missing keys"):
        client.analyze_failure("yaml", "logs", "CI", "org/repo")

@patch("boto3.client")
def test_invalid_json_raises(mock_boto):
    """Non-JSON Bedrock output must raise RuntimeError, not NameError."""
    from app.services.bedrock_client import BedrockRemediationClient

    mock_client = MagicMock()
    mock_client.invoke_model.return_value = _make_bedrock_response("not json at all")
    mock_client.exceptions.ThrottlingException = Exception
    mock_boto.return_value = mock_client

    client = BedrockRemediationClient()
    with pytest.raises(RuntimeError):
        client.analyze_failure("yaml", "logs", "CI", "org/repo")

@patch("boto3.client")
def test_worker_decrypt_key_mismatch_raises(mock_boto):
    """decrypt_token with a wrong key must raise InvalidToken, not return garbage."""
    import base64, hashlib
    from cryptography.fernet import Fernet, InvalidToken
    from app.core.security import decrypt_token

    key_a = base64.urlsafe_b64encode(
        hashlib.sha256(b"pipelineiq-token-encryption-v1:key-A").digest()
    )
    fernet_a = Fernet(key_a)
    encrypted = fernet_a.encrypt(b"ghp_token").decode()

    import app.core.config as cfg
    original_key = cfg.settings.SECRET_KEY
    cfg.settings.SECRET_KEY = "key-B"
    try:
        with pytest.raises(InvalidToken):
            decrypt_token(encrypted)
    finally:
        cfg.settings.SECRET_KEY = original_key
