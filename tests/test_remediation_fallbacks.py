import os
from unittest.mock import MagicMock, patch

os.environ.setdefault("DATABASE_URL", "postgresql://x:x@localhost/x")
os.environ.setdefault("SECRET_KEY", "test-secret-only")


def test_recover_suggested_yaml_uses_single_agent_when_direct_fix_is_invalid():
    from app.tasks.remediation import _recover_suggested_yaml

    mock_client = MagicMock()
    mock_client.generate_yaml_fix.return_value = "not: [valid"
    mock_client.analyze_failure.return_value = {
        "fixed_yaml": (
            "name: CI\n"
            "on: [push]\n"
            "jobs:\n"
            "  test:\n"
            "    runs-on: ubuntu-latest\n"
            "    steps:\n"
            "      - run: echo ok\n"
        ),
    }

    with patch("app.tasks.remediation.BedrockRemediationClient", return_value=mock_client):
        recovered = _recover_suggested_yaml(
            workflow_yaml=(
                "name: CI\n"
                "on: [push]\n"
                "jobs:\n"
                "  test:\n"
                "    runs-on: ubuntu-latest\n"
                "    steps:\n"
                "      - run: echo fail\n"
            ),
            root_cause="Python version is invalid",
            failure_category="DEPENDENCY_VERSION",
            logs="setup-python could not find version 99",
            workflow_name="CI",
            repo_full_name="Stagecraft-Ops/stagecraft-api",
        )

    assert recovered is not None
    assert "jobs:" in recovered


def test_recover_suggested_yaml_returns_none_when_all_fallbacks_are_invalid():
    from app.tasks.remediation import _recover_suggested_yaml

    mock_client = MagicMock()
    mock_client.generate_yaml_fix.return_value = "```yaml\nname: CI\n```"
    mock_client.analyze_failure.return_value = {"fixed_yaml": "name: CI"}

    with patch("app.tasks.remediation.BedrockRemediationClient", return_value=mock_client):
        recovered = _recover_suggested_yaml(
            workflow_yaml="name: CI\njobs:\n  test:\n    runs-on: ubuntu-latest\n",
            root_cause="Unknown failure",
            failure_category="UNKNOWN",
            logs="",
            workflow_name="CI",
            repo_full_name="Stagecraft-Ops/stagecraft-api",
        )

    assert recovered is None
