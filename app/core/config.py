from pydantic_settings import BaseSettings, SettingsConfigDict

INSECURE_DEFAULT_SECRET = "dev-insecure-secret-change-me"

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    AWS_REGION: str = "us-east-1"
    SQS_QUEUE_URL: str = "https://sqs.us-east-1.amazonaws.com/123456789/agora-webhooks"
    BEDROCK_MODEL_ID: str = "amazon.nova-pro-v1:0"

    # Cross-account Bedrock access (Bedrock account).
    # When set, the worker assumes this role before every Bedrock call.
    # Leave empty to call Bedrock directly with the pod's IRSA role (same account).
    BEDROCK_CROSS_ACCOUNT_ROLE_ARN: str = ""

    GITHUB_TOKEN: str = ""

    # MCP enrichment is optional. The worker already fetches the workflow and
    # failure logs, so remediation must work if this separate service is down.
    USE_MCP_TOOLS: bool = False
    MCP_GITHUB_URL: str = "http://agora-mcp-github.agora.svc.cluster.local:8010/sse"
    MCP_TOOL_TIMEOUT_SECONDS: float = 15.0

    DATABASE_URL: str = "postgresql://agora:password@postgres:5432/agora"
    REDIS_URL: str = "redis://redis:6379/0"

    SECRET_KEY: str = INSECURE_DEFAULT_SECRET
    TOKEN_ENCRYPTION_KEY: str = ""

    USE_MULTI_AGENT: bool = True

    # Shared secret checked on POST /internal/investigate (health.py) — the
    # Investigator Agent's entry point, called synchronously by agora-api's
    # chat endpoint. Same key, same purpose as agora-api's own
    # INTERNAL_API_KEY (gates its /internal/remediations/search route).
    INTERNAL_API_KEY: str = ""

    # "AI suggested a fix" email notification (SES). Sent to the org
    # owner's email after a remediation reaches status=analyzed. Best-effort
    # — a missing/unverified SES identity must never fail the remediation
    # itself. FRONTEND_URL builds the link to view the fix in the dashboard.
    SES_ENABLED: bool = False
    SES_FROM_EMAIL: str = ""
    FRONTEND_URL: str = "http://localhost:3000"

    # Bedrock Knowledge Base — replaces the pgvector log_embeddings pipeline.
    # Worker writes remediation docs to KB_S3_BUCKET; Bedrock ingests from S3.
    # Leave empty to fall back to the legacy pgvector path.
    BEDROCK_KB_ID: str = ""
    BEDROCK_KB_S3_BUCKET: str = ""

    # Bedrock Guardrail — applied to all converse() / InvokeModel calls.
    # Leave empty to skip guardrail (dev default until first terraform apply).
    BEDROCK_GUARDRAIL_ID: str = ""
    BEDROCK_GUARDRAIL_VERSION: str = ""


settings = Settings()
