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


settings = Settings()
