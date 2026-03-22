from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import ExecutionMode, InvocationMode, LoadMode

PROJECT_ROOT = Path(__file__).parent.parent
DBT_EXECUTABLE = str(PROJECT_ROOT / ".venv" / "bin" / "dbt")
TARGET = os.environ.get("DBT_TARGET", "dev")

project_config = ProjectConfig(
    dbt_project_path=PROJECT_ROOT,
    manifest_path=PROJECT_ROOT / "target" / "manifest.json",
)

profile_config = ProfileConfig(
    profile_name="snowflake_nyc_taxi",
    target_name=TARGET,
    profiles_yml_filepath=PROJECT_ROOT / "profiles.yml",
)

invocation_mode = (
    InvocationMode.DBT_RUNNER
    if os.environ.get("AIRFLOW_ENV") == "prod"
    else InvocationMode.SUBPROCESS
)
# Using Invocation method as SUBPROCESS since I'm testing out the orchestration in my local environment.
execution_config = ExecutionConfig(
    execution_mode=ExecutionMode.LOCAL,
    dbt_executable_path=DBT_EXECUTABLE,
    invocation_mode=invocation_mode,
)

render_config = RenderConfig(
    load_method=LoadMode.DBT_MANIFEST,
    select=["path:models"],
)

with DAG(
    dag_id="nyc_taxi_pipeline",
    description="Daily ELT: seed → dbt models → snapshot → test",
    schedule="0 6 * * *",
    start_date=datetime(2026, 3, 10, tzinfo=timezone.utc),
    catchup=False,
    default_args={
        "owner": "data-engineering",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": False,
    },
    tags=["nyc-taxi", "dbt", "snowflake"],
) as dag:

    dbt_cmd = f"{DBT_EXECUTABLE} {{}} --profiles-dir {PROJECT_ROOT} --project-dir {PROJECT_ROOT} --target {TARGET}"

    seed_task = BashOperator(
        task_id="dbt_seed",
        bash_command=dbt_cmd.format("seed"),
    )

    dbt_models = DbtTaskGroup(
        group_id="dbt_models",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=render_config,
        operator_args={"install_deps": False},
    )

    snapshot_task = BashOperator(
        task_id="dbt_snapshot",
        bash_command=dbt_cmd.format("snapshot"),
    )

    test_task = BashOperator(
        task_id="dbt_test",
        bash_command=dbt_cmd.format("test"),
    )

    seed_task >> dbt_models >> snapshot_task >> test_task
