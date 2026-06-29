# stagecraft-worker

Celery-based worker for Stagecraft. Runs two processes:
- **worker**: picks up Celery tasks from Redis and executes AI analysis via AWS Bedrock
- **consumer**: long-polls SQS, dispatches messages to Celery tasks (the SQS→Celery bridge)

**Part of**: [Stagecraft-Ops](https://github.com/Stagecraft-Ops)

## Quick start

```bash
cp .env.example .env
docker compose up --build
```
