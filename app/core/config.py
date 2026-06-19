from pydantic_settings import BaseSettings, SettingsConfigDict

INSECURE_DEFAULT_SECRET = "dev-insecure-secret-change-me"

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    AWS_REGION: str = "us-east-1"
    SQS_QUEUE_URL: str = "https://sqs.us-east-1.amazonaws.com/123456789/agora-webhooks"
    BEDROCK_MODEL_ID: str = "anthropic.claude-3-5-sonnet-20241022-v2:0"

    GITHUB_TOKEN: str = ""

    DATABASE_URL: str = "postgresql://agora:password@postgres:5432/agora"
    REDIS_URL: str = "redis://redis:6379/0"

    SECRET_KEY: str = INSECURE_DEFAULT_SECRET
    TOKEN_ENCRYPTION_KEY: str = ""

    USE_MULTI_AGENT: bool = False

    BEDROCK_AGENT_ID_CLASSIFIER: str = ""
    BEDROCK_AGENT_ID_ROOT_CAUSE: str = ""
    BEDROCK_AGENT_ID_YAML_FIXER: str = ""
    BEDROCK_AGENT_ID_SECURITY_REVIEWER: str = ""
    BEDROCK_AGENT_ID_PR_WRITER: str = ""

    BEDROCK_AGENT_ALIAS_ID_CLASSIFIER: str = "TSTALIASID"
    BEDROCK_AGENT_ALIAS_ID_ROOT_CAUSE: str = "TSTALIASID"
    BEDROCK_AGENT_ALIAS_ID_YAML_FIXER: str = "TSTALIASID"
    BEDROCK_AGENT_ALIAS_ID_SECURITY_REVIEWER: str = "TSTALIASID"
    BEDROCK_AGENT_ALIAS_ID_PR_WRITER: str = "TSTALIASID"

    @property
    def BEDROCK_AGENT_IDS(self) -> dict:
        return {
            "classifier": self.BEDROCK_AGENT_ID_CLASSIFIER,
            "root_cause": self.BEDROCK_AGENT_ID_ROOT_CAUSE,
            "yaml_fixer": self.BEDROCK_AGENT_ID_YAML_FIXER,
            "security_reviewer": self.BEDROCK_AGENT_ID_SECURITY_REVIEWER,
            "pr_writer": self.BEDROCK_AGENT_ID_PR_WRITER,
        }

    @property
    def BEDROCK_AGENT_ALIAS_IDS(self) -> dict:
        return {
            "classifier": self.BEDROCK_AGENT_ALIAS_ID_CLASSIFIER,
            "root_cause": self.BEDROCK_AGENT_ALIAS_ID_ROOT_CAUSE,
            "yaml_fixer": self.BEDROCK_AGENT_ALIAS_ID_YAML_FIXER,
            "security_reviewer": self.BEDROCK_AGENT_ALIAS_ID_SECURITY_REVIEWER,
            "pr_writer": self.BEDROCK_AGENT_ALIAS_ID_PR_WRITER,
        }


settings = Settings()
