from datetime import datetime, timedelta
from pathlib import Path

import yaml
from airflow.sdk import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

_DAG_DIR = Path(__file__).parent
JOB_SPECS: list[dict] = yaml.safe_load((_DAG_DIR / "job_specs.yaml").read_text())["jobs"]

RUSTFS_SECRET_REF = k8s.V1EnvFromSource(
    secret_ref=k8s.V1SecretEnvSource(name="rustfs-s3-credentials")
)

WORKER_RESOURCES = k8s.V1ResourceRequirements(
    requests={"cpu": "250m", "memory": "512Mi"},
    limits={"cpu": "1", "memory": "2Gi"},
)

DEFAULT_ARGS = {
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "execution_timeout": timedelta(minutes=30),
    "owner": "platform-eng",
    "depends_on_past": False,
}

with DAG(
    dag_id="dummy_pipeline",
    schedule="0 2 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    tags=["dummy", "helical-dev"],
) as dag:
    for spec in JOB_SPECS:
        job_id: str = spec["job_id"]

        KubernetesPodOperator(
            task_id=f"dummy__{job_id.replace('-', '_')}",
            name=f"dummy-{job_id}",
            namespace="airflow",
            service_account_name="airflow-worker-sa",
            image="ttl.sh/helical-mlops-worker:12h",
            image_pull_policy="IfNotPresent",
            env_vars=[
                k8s.V1EnvVar(name="JOB_CONFIG_YAML", value=yaml.dump(spec, default_flow_style=False)),
            ],
            env_from=[RUSTFS_SECRET_REF],
            container_resources=WORKER_RESOURCES,
            get_logs=True,
            log_events_on_failure=True,
            is_delete_operator_pod=True,
            on_finish_action="delete_succeeded_pod",
            in_cluster=True,
            do_xcom_push=False,
        )
