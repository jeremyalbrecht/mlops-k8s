from datetime import datetime, timedelta
from pathlib import Path

import yaml
from airflow.sdk import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

_DAG_DIR = Path(__file__).parent
_JOB_SPECS_PATH = _DAG_DIR / "job_specs.yaml"


def _load_job_specs(path: Path) -> list[dict]:
    """Parse job_specs.yaml and return a list of job spec dicts."""
    if not path.exists():
        raise FileNotFoundError(
            f"job_specs.yaml not found at {path}. "
            "Ensure the file is present in the DAGs folder."
        )
    with path.open() as fh:
        config = yaml.safe_load(fh)
    return config["jobs"]


JOB_SPECS: list[dict] = _load_job_specs(_JOB_SPECS_PATH)

RUSTFS_VOLUME = k8s.V1Volume(
    name="rustfs-storage",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
        claim_name="rustfs-pvc"
    ),
)

RUSTFS_VOLUME_MOUNT = k8s.V1VolumeMount(
    name="rustfs-storage",
    mount_path="/data",
)

# Resource guardrails — sized conservatively for a single-node Minikube cluster.
WORKER_RESOURCES = k8s.V1ResourceRequirements(
    requests={"cpu": "250m", "memory": "512Mi"},
    limits={"cpu": "1",     "memory": "2Gi"},
)

DEFAULT_ARGS = {
    # Retry once after a 3-minute back-off before alerting on failure.
    # This absorbs transient node pressure or image-pull delays on Minikube.
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    # Hard deadline per task — prevents a hung pod from blocking the slot pool.
    "execution_timeout": timedelta(minutes=30),
    "owner": "platform-eng",
    "depends_on_past": False,
}

# ──────────────────────────────────────────────
# DAG definition
# ──────────────────────────────────────────────

with DAG(
    dag_id="dummy_pipeline",
    schedule_interval="0 2 * * *",   # nightly at 02:00 UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,                   # don't back-fill missed runs on first deploy
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    tags=["dummy", "helical-dev"],
) as dag:
    for spec in JOB_SPECS:
        job_id: str = spec["job_id"]
        task_id = f"dummy__{job_id.replace('-', '_')}"

        env_vars = [
            k8s.V1EnvVar(name="JOB_ID",         value=str(spec["job_id"])),
            k8s.V1EnvVar(name="BATCH_SIZE",      value=str(spec["batch_size"])),
            k8s.V1EnvVar(name="MODEL_NAME",      value=str(spec["model_name"])),
            k8s.V1EnvVar(name="INPUT_CSV_PATH",  value=str(spec["input_csv_path"])),
            k8s.V1EnvVar(name="TARGET_BUCKET",   value=str(spec["target_bucket"])),
        ]

        KubernetesPodOperator(
            # ── Identity ────────────────────────────
            task_id=task_id,
            name=f"dummy-{job_id}",   # pod name visible in `kubectl get pods`

            # ── Placement ───────────────────────────
            namespace="airflow",
            service_account_name="airflow-worker-sa",

            # ── Container spec ──────────────────────
            image="ttl.sh/helical-mlops-worker:12h",
            image_pull_policy="IfNotPresent",    # use locally cached image on Minikube
            env_vars=env_vars,
            container_resources=WORKER_RESOURCES,

            # ── Storage ─────────────────────────────
            volumes=[RUSTFS_VOLUME],
            volume_mounts=[RUSTFS_VOLUME_MOUNT],

            # ── Observability ───────────────────────
            get_logs=True,               # stream pod stdout/stderr into Airflow task logs
            log_events_on_failure=True,  # capture Kubernetes Events on failure

            # ── Lifecycle ───────────────────────────
            # Successful pods are deleted immediately to reclaim node resources.
            # Failed pods are intentionally kept so engineers can `kubectl logs`
            # or `kubectl exec` for post-mortem debugging.
            is_delete_operator_pod=True,
            on_finish_action="delete_succeeded_pod",

            # ── Connectivity (Minikube) ──────────────
            # in_cluster=True tells the operator to use the pod's own service-account
            # token rather than a kubeconfig file — correct for production and Minikube.
            in_cluster=True,
            do_xcom_push=False,          # inference workers don't return XCom payloads
        )

