import re

_PATTERNS = [
    (re.compile(r'(?i)(AKIA|ASIA|AROA)[A-Z0-9]{16}'), '[AWS_KEY_REDACTED]'),
    (re.compile(r'(?i)aws.{0,20}secret.{0,10}[\'\"]([A-Za-z0-9/+]{40})[\'\"]'), '[AWS_SECRET_REDACTED]'),
    (re.compile(r'(?i)(ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9_]{36,}'), '[GH_TOKEN_REDACTED]'),
    (re.compile(r'(?i)(password|passwd|pwd)\s*[=:]\s*\S+'), '[PASSWORD_REDACTED]'),
    (re.compile(r'(?i)(token|api[_-]?key|secret[_-]?key|auth[_-]?key)\s*[=:]\s*\S+'), '[SECRET_REDACTED]'),
    (re.compile(r'postgresql://[^@\s]+@'), 'postgresql://[REDACTED]@'),
    (re.compile(r'redis://:[^@\s]+@'), 'redis://:[REDACTED]@'),
    (re.compile(r'(?i)-----BEGIN [A-Z ]+PRIVATE KEY-----[\s\S]*?-----END [A-Z ]+PRIVATE KEY-----'), '[PRIVATE_KEY_REDACTED]'),
]

MAX_LOG_LINES = 300


def scrub(text: str) -> str:
    lines = text.splitlines()
    if len(lines) > MAX_LOG_LINES:
        lines = lines[-MAX_LOG_LINES:]
    result = '\n'.join(lines)
    for pattern, replacement in _PATTERNS:
        result = pattern.sub(replacement, result)
    return result
