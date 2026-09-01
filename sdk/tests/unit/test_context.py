import pytest
from lakehouse.context import Process, resolve_actor, resolve_platform, resolve_process

ALL_VARS = (
    "AIRFLOW_CTX_DAG_ID",
    "AIRFLOW_CTX_DAG_RUN_ID",
    "AIRFLOW_CTX_TASK_ID",
    "LAKEHOUSE_PROCESS_KIND",
    "LAKEHOUSE_PROCESS_NAME",
    "LAKEHOUSE_PROCESS_RUN_ID",
)


@pytest.mark.parametrize(
    ("env", "expected"),
    [
        (
            {
                "AIRFLOW_CTX_DAG_ID": "dag",
                "AIRFLOW_CTX_DAG_RUN_ID": "run",
                "AIRFLOW_CTX_TASK_ID": "t",
            },
            Process("pipeline", "dag", "run/t"),
        ),
        (
            {"AIRFLOW_CTX_DAG_ID": "dag", "AIRFLOW_CTX_DAG_RUN_ID": "run"},
            Process("pipeline", "dag", "run"),
        ),
        (
            {
                "LAKEHOUSE_PROCESS_KIND": "model_run",
                "LAKEHOUSE_PROCESS_NAME": "churn",
                "LAKEHOUSE_PROCESS_RUN_ID": "e42",
            },
            Process("model_run", "churn", "e42"),
        ),
        # airflow context wins over explicit process env
        (
            {"AIRFLOW_CTX_DAG_ID": "dag", "LAKEHOUSE_PROCESS_KIND": "model_run"},
            Process("pipeline", "dag", None),
        ),
        ({}, Process("adhoc")),
    ],
)
def test_resolve_process(monkeypatch, env, expected):
    for var in ALL_VARS:
        monkeypatch.delenv(var, raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    assert resolve_process() == expected


def test_resolve_actor_env_wins(monkeypatch):
    monkeypatch.setenv("LAKEHOUSE_ACTOR", "svc-ingest")
    assert resolve_actor() == "svc-ingest"


def test_resolve_actor_falls_back_to_os_user(monkeypatch):
    monkeypatch.delenv("LAKEHOUSE_ACTOR", raising=False)
    assert resolve_actor()  # whatever getpass returns, it must be non-empty


@pytest.mark.parametrize(("env_value", "expected"), [(None, "local"), ("aws", "aws")])
def test_resolve_platform(monkeypatch, env_value, expected):
    if env_value is None:
        monkeypatch.delenv("LAKEHOUSE_PLATFORM", raising=False)
    else:
        monkeypatch.setenv("LAKEHOUSE_PLATFORM", env_value)
    assert resolve_platform() == expected
