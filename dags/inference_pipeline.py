from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
import asyncio

import yaml
from airflow.sdk import DAG, AsyncCallback, DeadlineAlert, DeadlineReference
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.utils.email import send_email
from kubernetes.client import models as k8s


_DAG_DIR = Path(__file__).parent
_SUPPORTED_KIND = "InferencePipelineConfig"

_DEFAULT_IMAGE = "ttl.sh/helical-mlops-worker:12h"
_DEFAULT_SERVICE_ACCOUNT = "airflow-worker-sa"
_DEFAULT_NAMESPACE = "airflow"
_DEFAULT_RETRIES = 1
_DEFAULT_TIMEOUT_MINUTES = 30
_DEFAULT_RESOURCES = {
    "requests": {"cpu": "250m", "memory": "512Mi"},
    "limits": {"cpu": "1", "memory": "2Gi"},
}

RUSTFS_SECRET_REF = k8s.V1EnvFromSource(
    secret_ref=k8s.V1SecretEnvSource(name="rustfs-s3-credentials")
)

@dataclass
class JobConfig:
    """Version-agnostic job description passed to KubernetesPodOperator."""

    job_id: str
    batch_size: int
    model_name: str
    input_csv: str
    output_bucket: str
    image: str = _DEFAULT_IMAGE
    service_account_name: str = _DEFAULT_SERVICE_ACCOUNT
    retries: int = _DEFAULT_RETRIES
    timeout_minutes: int = _DEFAULT_TIMEOUT_MINUTES
    resources: dict[str, Any] = field(default_factory=lambda: dict(_DEFAULT_RESOURCES))
    description: str = ""


@dataclass
class PipelineConfig:
    """Top-level object returned by the loader regardless of apiVersion."""

    pipeline_name: str
    api_version: str
    jobs: list[JobConfig]


def _parse_v1alpha1(raw: dict) -> PipelineConfig:
    jobs = [
        JobConfig(
            job_id=j["job_id"],
            batch_size=j["batch_size"],
            model_name=j["model_name"],
            input_csv=j["input_csv"],
            output_bucket=j["output_bucket"],
        )
        for j in raw["spec"]["jobs"]
    ]
    return PipelineConfig(
        pipeline_name=raw["metadata"]["name"],
        api_version=raw["apiVersion"],
        jobs=jobs,
    )


def _parse_v1beta1(raw: dict) -> PipelineConfig:
    spec = raw["spec"]

    pipeline_image = spec.get("image", _DEFAULT_IMAGE)
    pipeline_sa = spec.get("serviceAccountName", _DEFAULT_SERVICE_ACCOUNT)

    pipeline_defaults = spec.get("defaults", {})
    default_retries = pipeline_defaults.get("retries", _DEFAULT_RETRIES)
    default_timeout = pipeline_defaults.get("timeoutMinutes", _DEFAULT_TIMEOUT_MINUTES)
    default_resources = pipeline_defaults.get("resources", _DEFAULT_RESOURCES)

    jobs: list[JobConfig] = []
    for j in spec["jobs"]:
        jobs.append(
            JobConfig(
                job_id=j["job_id"],
                batch_size=j["batch_size"],
                model_name=j["model_name"],
                input_csv=j["input_csv"],
                output_bucket=j["output_bucket"],
                description=j.get("description", ""),
                # Per-job overrides fall back to pipeline-level defaults.
                image=j.get("image", pipeline_image),
                service_account_name=j.get("serviceAccountName", pipeline_sa),
                retries=j.get("retries", default_retries),
                timeout_minutes=j.get("timeoutMinutes", default_timeout),
                resources=j.get("resources", default_resources),
            )
        )

    return PipelineConfig(
        pipeline_name=raw["metadata"]["name"],
        api_version=raw["apiVersion"],
        jobs=jobs,
    )


async def deadline_alert_callback(**kwargs) -> None:
    """Called by the Airflow triggerer when a DeadlineAlert interval is exceeded."""
    dag_run = kwargs.get("context", {}).get("dag_run", {})
    dag_id = dag_run.get("dag_id", "unknown")
    dag_run_id = dag_run.get("dag_run_id", "unknown")
    subject = f"[Airflow] Deadline missed — {dag_id}"
    msg = (
        f"<p>Deadline alert triggered for DAG <strong>{dag_id}</strong>.</p>"
        f"<p>Run ID: {dag_run_id}</p>"
    )
    await asyncio.to_thread(
        send_email, to=["me@jalbrecht.fr"], subject=subject, html_content=msg
    )

deadline_alert_callback.__module__ = "inference_pipeline"


_PARSERS = {
    "mlops.helical.dev/v1alpha1": _parse_v1alpha1,
    "mlops.helical.dev/v1beta1": _parse_v1beta1,
}


def load_pipeline_config(path: Path) -> PipelineConfig:
    """Load and validate a versioned InferencePipelineConfig YAML file."""
    raw = yaml.safe_load(path.read_text())

    kind = raw.get("kind")
    if kind != _SUPPORTED_KIND:
        raise ValueError(f"{path.name}: unsupported kind '{kind}', expected '{_SUPPORTED_KIND}'")

    api_version = raw.get("apiVersion", "")
    parser = _PARSERS.get(api_version)
    if parser is None:
        supported = ", ".join(_PARSERS)
        raise ValueError(
            f"{path.name}: unsupported apiVersion '{api_version}'. "
            f"Supported: {supported}"
        )

    return parser(raw)


_SPEC_FILES = [
    (path.stem, path)
    for path in sorted(_DAG_DIR.glob("job_specs*.yaml"))
]

_BASE_DEFAULT_ARGS = {
    "retry_delay": timedelta(minutes=3),
    "owner": "platform-eng",
    "depends_on_past": False,
}

for _dag_id, _spec_path in _SPEC_FILES:
    _config = load_pipeline_config(_spec_path)

    with DAG(
        dag_id=_dag_id,
        schedule="0 2 * * *",
        start_date=datetime(2025, 1, 1),
        catchup=False,
        default_args={
            **_BASE_DEFAULT_ARGS,
            "retries": _config.jobs[0].retries if _config.jobs else _DEFAULT_RETRIES,
        },
        deadline=DeadlineAlert(
            reference=DeadlineReference.DAGRUN_QUEUED_AT,
            interval=timedelta(seconds=40),
            callback=AsyncCallback(deadline_alert_callback),
        ),
        dagrun_timeout=timedelta(hours=2),
        tags=["helical-mlops", _config.api_version.replace("/", "-")],
        doc_md=f"Pipeline `{_config.pipeline_name}` loaded from `{_spec_path.name}` "
               f"(`{_config.api_version}`).",
    ) as _dag:
        for _job in _config.jobs:
            _resources = k8s.V1ResourceRequirements(
                requests=_job.resources.get("requests", {}),
                limits=_job.resources.get("limits", {}),
            )

            KubernetesPodOperator(
                task_id=f"infer__{_job.job_id.replace('-', '_')}",
                name=f"infer-{_job.job_id}",
                namespace=_DEFAULT_NAMESPACE,
                service_account_name=_job.service_account_name,
                image=_job.image,
                image_pull_policy="IfNotPresent",
                env_vars=[
                    k8s.V1EnvVar(
                        name="JOB_CONFIG_YAML",
                        value=yaml.dump(
                            {
                                "job_id": _job.job_id,
                                "batch_size": _job.batch_size,
                                "model_name": _job.model_name,
                                "input_csv": _job.input_csv,
                                "output_bucket": _job.output_bucket,
                            },
                            default_flow_style=False,
                        ),
                    ),
                ],
                env_from=[RUSTFS_SECRET_REF],
                container_resources=_resources,
                retries=_job.retries,
                execution_timeout=timedelta(minutes=_job.timeout_minutes),
                get_logs=True,
                log_events_on_failure=True,
                is_delete_operator_pod=True,
                on_finish_action="delete_succeeded_pod",
                in_cluster=True,
                do_xcom_push=False,
            )

    globals()[_dag_id] = _dag
