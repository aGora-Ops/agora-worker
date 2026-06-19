# agora-worker

Celery-based worker for aGorA. Runs two processes:
- **worker**: picks up Celery tasks from Redis and executes AI analysis via AWS Bedrock
- **consumer**: long-polls SQS, dispatches messages to Celery tasks (the SQS→Celery bridge)

**Part of**: [aGora-Ops](https://github.com/aGora-Ops)

## Quick start

```bash
cp .env.example .env
docker compose up --build
```
