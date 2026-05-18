# MLOps Pipeline Orchestration with Kubernetes & Airflow - Platform Engineering Challenge

Batch inference pipeline on Minikube using Apache Airflow (KubernetesExecutor), ArgoCD (GitOps), and RustFS (S3-compatible object storage).

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Minikube                             │
│                                                             │
│  ┌──────────┐   ApplicationSet   ┌──────────┐              │
│  │  ArgoCD  │ ─────────────────► │  Airflow │              │
│  │          │                    │(airflow/)│              │
│  │          │ ─────────────────► │  RustFS  │              │
│  └──────────┘                    │(rustfs/) │              │
│                                  └──────────┘              │
│                                                             │
│  Airflow DAG ──► KubernetesPodOperator ──► worker pod      │
│                        │                       │           │
│                  reads job_specs*.yaml    reads CSV from    │
│                  at DAG parse time        RustFS, writes    │
│                                           .npy back         │
└─────────────────────────────────────────────────────────────┘
```

## Repository Structure

| Path | Contents                                                                                                                |
|------|-------------------------------------------------------------------------------------------------------------------------|
| `applications/` | ArgoCD Application manifests for Airflow, RustFS, and ArgoCD itself. Each folder generates a new application in Argo.   |
| `cluster/` | ArgoCD `ApplicationSet`: single entry point for the whole platform. One YAML that allows to automatically generate all. |
| `dags/` | DAG definition + versioned `InferencePipelineConfig` job specs                                                          |
| `worker/` | Dockerfile, entrypoint, and inference logic                                                                             |
| `scripts/` | Bootstrap (`start.sh`) and Terraform bucket provisioning (`tf_init.sh`)                                                 |
| `terraform/` | OpenTofu config to create S3 buckets on RustFS                                                                          |

## Prerequisites

- [minikube](https://minikube.sigs.k8s.io/) ≥ v1.35
- [kubectl](https://kubernetes.io/docs/tasks/tools/) (aliased as `k` in the scripts)
- [OpenTofu](https://opentofu.org/) ≥ v1.9 (Terraform should also work, but I preferred OpenTofu for licensing reasons)

## Quick Start

### 1. Bootstrap the cluster

```bash
bash scripts/start.sh
```

This script:
1. Starts Minikube with 4 CPUs / 8 GB RAM and enables the `ingress` add-on (required later to access the UIs)
2. Installs ArgoCD in the `argocd` namespace
3. Applies `cluster/application-set.yaml`: ArgoCD then automatically deploys **Airflow** and **RustFS** from this repository

Wait for all pods to be ready (~3–5 minutes):
```bash
k get pods -n airflow
k get pods -n rustfs
```

### 2. Provision S3 buckets

```bash
bash scripts/tf_init.sh
```

Creates the `demo-bucket` (input) bucket on RustFS using OpenTofu. 

### 3. Build and push the worker image

The DAG references `ttl.sh/helical-mlops-worker:12h`. The image is automatically built by a GitHub actions workflow on any push to the main branch.
I am using ttl.sh as an image registry. Images on it are stored for the duration of their tag, it is a useful platform to allow for quick PoC. 

### 4. Configure `/etc/hosts` for ingress access

Minikube exposes ingresses via `minikube tunnel`. Run it in a separate terminal:
```bash
minikube tunnel
```

Then add the following entries to `/etc/hosts` (requires `sudo`):
```
# Helical MLOps
127.0.0.1  airflow.helical.dev
127.0.0.1  argocd.helical.dev
127.0.0.1  s3.helical.dev
127.0.0.1  rustfs.helical.dev
```

Edit with:
```bash
sudo nano /etc/hosts
# or
sudo sh -c 'echo "127.0.0.1  airflow.helical.dev argocd.helical.dev s3.helical.dev rustfs.helical.dev" >> /etc/hosts'
```

### 5. Access the UIs

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://airflow.helical.dev | `admin` / `admin` |
| ArgoCD | http://argocd.helical.dev | `admin` / `kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath='{.data.password}' \| base64 -d` |
| RustFS Console | http://rustfs.helical.dev | `rustfsadmin` / `rustfsadmin` |
| RustFS S3 API | http://s3.helical.dev | - |


### 6. Upload sample data

1. Open the RustFS console at **http://rustfs.helical.dev** (`rustfsadmin` / `rustfsadmin`)
2. Navigate to **Buckets → demo-bucket** (created by Terraform in the previous step)
3. Click **Upload** and select `dummy_input.csv` from the repository root

### 7. Trigger the DAG

In the Airflow UI, enable and trigger one of:
- `job_specs_v1alpha1` -> three simple batch jobs from `demo-bucket`.
- `job_specs_v1beta1` -< three production-style jobs with per-job resource/retry overrides but with a misconfigured image name.

### 8. Trigger Deadline Alerts

The DAG uses `SmtpNotifier` to send an email when the deadline is breached. Configure SMTP via an Airflow Connection.

**In the Airflow UI (`http://airflow.helical.dev`):**

1. Go to **Admin → Connections → +**
2. Fill in the fields:

| Field | Value |
|-------|-------|
| Connection Id | `smtp_default` |
| Connection Type | `Email` |
| Host | your SMTP host (e.g. `smtp.gmail.com`) |
| Login | your email address |
| Password | your app password |
| Port | `587` |

3. Save.

> Airflow's `SmtpNotifier` resolves SMTP credentials from the `smtp_default` connection by convention. See [Airflow SMTP notification docs](https://airflow.apache.org/docs/apache-airflow-providers-smtp/stable/notifications/smtp_notifier_howto_guide.html).

**To trigger the alert:**

The worker deliberately sleeps 60 seconds ~50% of the time, exceeding the 40-second `DeadlineAlert` window. Trigger `job_specs_v1alpha1` from the Airflow UI and wait if the worker hits the slow path, the alert fires and you'll receive an email within seconds of the deadline breach.

## Observability

### Retrieve worker pod logs

Worker pods are named `infer-<job_id>-*` and run in the `airflow` namespace:
```bash
# List pods (including completed)
kubectl get pods -n airflow

# Tail logs for a specific worker
kubectl logs -f -n airflow <pod-name>
```

### Retry policy and deadline alerts

Each DAG has:
- **Per-task retries** - configured in the job spec (`retries` field, default `1`), with a 3-minute retry delay
- **`dagrun_timeout`** - hard 2-hour cap on the entire DAG run
- **`DeadlineAlert`** - Airflow 3 deadline configured at 40 seconds from queue time; triggers an email on breach (useful to catch stuck pods early)

The worker deliberately sleeps 60 seconds ~50% of the time to trigger the deadline alert in demo runs.

---

## Design Decisions & Differences from the Assignment

### RustFS instead of MinIO

The assignment specified MinIO. This implementation uses [RustFS](https://rustfs.com), an S3-compatible object store written in Rust, API-compatible with MinIO's S3 and console interfaces. The worker uses the `minio` Python SDK pointed at the RustFS endpoint - no code change required. 

The reasoning: RustFS has a leaner footprint on Minikube, moreover I will not choose anymore MinIO as a solution if the company is not considering to pay for the licences. MiniIO is now in maintenance mode and the repo is read-only, in favor for their commercial solution.

### GitOps via ArgoCD instead of raw Helm installs

The assignment asked for Helm values files and manual `helm install` commands. This implementation introduces ArgoCD with an `ApplicationSet` that discovers every folder under `applications/` and deploys it automatically.

> ArgoCD's [ApplicationSet Git directory generator](https://argo-cd.readthedocs.io/en/stable/operator-manual/applicationset/Generators-Git/#git-generator-directories) eliminates per-app `helm install` commands and makes the cluster self-healing: any drift is automatically corrected, and there is no room for manual patching by the on-call guy that get lost on the next upgrade, re-causing the same issue.

This means:
- `applications/airflow/application.yaml` → Helm release for Airflow (chart `airflow`, version `1.21.0`)
- `applications/rustfs/application.yaml` → Helm release for RustFS (chart `rustfs`, version `0.3.0`)
- `applications/argocd/kustomization.yaml` → ArgoCD manages itself (patched to run `--insecure` for the ingress)

### OpenTofu for bucket provisioning

The assignment implied bucket creation via scripts or Helm post-install hooks. Using OpenTofu (open-source Terraform) with the `hashicorp/aws` provider pointed at the RustFS S3 endpoint gives declarative, idempotent bucket management. In that case it is just 3 lines to declare the buckets, but I wanted to demonstrate the usage of the AWS provider our RustFS endpoint, and the need for everything to be declared as code.

### Versioned `InferencePipelineConfig` spec format

The assignment asked for a flat job list in a config file or Airflow Variable. This implementation introduces a Kubernetes-inspired versioned manifest format (`mlops.helical.dev/v1alpha1` / `v1beta1`) for job specs. `v1beta1` adds pipeline-level defaults and per-job overrides for image, resources, retries, and timeout, making the format extensible without breaking existing specs. This allows for ML/AI Engineer to not need to perpetually update their Job specifications if the Platform Engineer updates it and allow for quickly delivering new features without breaking existing config.

### `LocalExecutor + KubernetesExecutor` (dual executor)

Airflow is configured with `executor: "LocalExecutor,KubernetesExecutor"`. Airflow 3 supports [hybrid executors](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/index.html#hybrid-executor) - tasks annotated with `queue="kubernetes"` (the default for `KubernetesPodOperator`) run in pods; lightweight internal tasks can run locally. This is needed to allow for DeadlineAlerts to be sent. 

### `DeadlineAlert` instead of `sla_miss_callback`

The assignment mentioned SLA miss configuration. `sla_miss_callback` is deprecated in Airflow 3. The implementation uses the new [`DeadlineAlert`](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/deadlines.html) API (`DeadlineReference.DAGRUN_QUEUED_AT`) which triggers an async email callback, replacing the old SLA mechanism cleanly.

### `k8s/` → `applications/` + `cluster/`

The assignment expected a single `k8s/` directory. The implementation splits concerns:
- `cluster/` - cluster-bootstrap level (ArgoCD ApplicationSet)
- `applications/<app>/` - per-application manifests (Helm values, RBAC, Ingress, Secrets)

This matches the [App of Apps pattern](https://argo-cd.readthedocs.io/en/stable/operator-manual/cluster-bootstrapping/) recommended by ArgoCD.

---

## Potential Next Steps

### 1. Bucket Access Permissions - Team-scoped Storage Isolation

Currently all worker pods share the same `rustfs-s3-credentials` Secret, giving every job unrestricted access to every bucket. The target model is: **one team = one Git repository = one S3 bucket = one Kubernetes namespace**, with the pipeline enforcing that a job can only reach the bucket that belongs to its own namespace.

**Proposed implementation:**

- Create one namespace per team (e.g. `team-alpha`, `team-beta`).
- Provision a dedicated RustFS bucket per team via a new Terraform module (extend `terraform/main.tf`). Generate a scoped access key/secret pair for each bucket using the [RustFS admin API](https://min.io/docs/minio/linux/administration/identity-access-management/policy-based-access-control.html) (API-compatible with MinIO IAM).
- Store the scoped credentials in a `Secret` inside the team namespace. The `KubernetesPodOperator` in the DAG mounts `env_from` pointing at the Secret in its own namespace. If the Secret doesn't exist (because the bucket isn't in scope), the pod fails to start with a clear `CreateContainerConfigError`, making the access boundary explicit rather than silent.
- Enforce this at the RBAC level: the `airflow-worker-sa` `Role` (already scoped per namespace) only grants `get` on `secrets` within its own namespace, so cross-namespace Secret reads are impossible even if a DAG were misconfigured.

```
team-alpha namespace
  └── Secret: rustfs-s3-credentials  →  bucket: team-alpha-data  (read+write)
  └── Airflow DAG job_specs_team_alpha.yaml  →  input_csv: team-alpha-data/...

team-beta namespace
  └── Secret: rustfs-s3-credentials  →  bucket: team-beta-data   (read+write)
  └── KubernetesPodOperator mounts team-beta Secret only
       → attempting to access team-alpha-data returns 403 at the S3 layer
```

---

### 2. OpenTelemetry Instrumentation

The pipeline currently has no distributed tracing or structured metrics beyond Airflow's built-in task state. Adding OpenTelemetry would allow correlating scheduler latency, pod scheduling delay, and inference runtime in a single trace per job run.

**Proposed implementation:**

**Airflow side:**  
Airflow 2.10+ ships with [built-in OpenTelemetry metrics export](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/metrics.html#setup-opentelemetry). Enable it in the Helm values:
```yaml
config:
  metrics:
    otel_on: "True"
    otel_host: "otel-collector.observability.svc.cluster.local"
    otel_port: "4318"
    otel_prefix: "airflow"
```
This exposes counters and histograms for DAG run duration, task scheduling latency, and executor queue depth without any code change.

**Worker side:**  
Instrument `worker.py` directly using the [OpenTelemetry Python SDK](https://opentelemetry.io/docs/languages/python/):
```python
from opentelemetry import trace, metrics
from opentelemetry.sdk.trace import TracerProvider

tracer = trace.get_tracer("ml-worker")

with tracer.start_as_current_span("inference_job") as span:
    span.set_attribute("job.id", job_id)
    span.set_attribute("job.model", model_name)
    span.set_attribute("job.api_version", api_version)   # v1alpha1 / v1beta1
    span.set_attribute("job.batch_size", batch_size)
    # ... inference logic
```

Custom metrics to add:
| Metric | Type | Why |
|--------|------|-----|
| `worker.inference.duration_seconds` | Histogram | Actual model runtime per job |
| `worker.queue_wait_seconds` | Gauge | Time from DAG trigger to pod start (passed as env var by the DAG) |
| `airflow.dag.api_version` | Counter | Track `v1alpha1` vs `v1beta1` adoption across runs |
| `worker.csv.row_count` | Histogram | Input data size distribution |

Deploy an [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/) in the cluster (lightweight `otelcol-contrib` Helm chart) configured to export to Prometheus (for Grafana dashboards) and Tempo or Jaeger (for traces). All three (Airflow metrics, worker traces, and Kubernetes pod metrics) converge in a single observability stack.

---

### 3. Worker Image Security / Versioned Tags + Digest Pinning

The current DAG references `ttl.sh/helical-mlops-worker:12h` - an ephemeral registry with a mutable, time-limited tag. This is acceptable for a demo but is a supply chain risk in any environment beyond local development:

- A mutable tag can be silently overwritten; the cluster may pull a different image than what was tested.
- `ttl.sh` provides no image signing or provenance attestation.

**Proposed implementation:**

**1. Semantic versioned tags in CI:**  
Tag the worker image by Git SHA and semver on every merge to `master` using a GitHub Actions workflow:
```yaml
- name: Build and push
  uses: docker/build-push-action@v6
  with:
    tags: |
      ghcr.io/jeremyalbrecht/helical-mlops-worker:${{ github.sha }}
      ghcr.io/jeremyalbrecht/helical-mlops-worker:1.2.3
```

Publish to a dedicated image registry.

**2. Pin by digest in the DAG:**  
Instead of a tag (mutable), reference the image by its immutable SHA-256 digest:
```python
_DEFAULT_IMAGE = "ghcr.io/jeremyalbrecht/helical-mlops-worker@sha256:abc123..."
```
> Kubernetes documentation explicitly recommends [using image digests over tags](https://kubernetes.io/docs/concepts/containers/images/#image-pull-policy) to guarantee that the exact same image layer is pulled on every run. A tag can point to a different digest after a push; a digest never changes.

**3. Sign the image with Sigstore Cosign:**  
Use [cosign](https://docs.sigstore.dev/cosign/signing/signing_with_containers/) to sign the image after push and verify the signature in CI before deploying:
```bash
cosign sign --key cosign.key ghcr.io/jeremyalbrecht/helical-mlops-worker@sha256:abc123...
cosign verify --key cosign.pub ghcr.io/jeremyalbrecht/helical-mlops-worker@sha256:abc123...
```
Add a Kubernetes [admission webhook](https://kubernetes.io/docs/reference/access-authn-authz/admission-controllers/) (e.g. [Kyverno](https://kyverno.io/)) that rejects any pod whose image digest is not signed by the known key, preventing unsigned or tampered images from running even if someone bypasses CI.

**4. Automate base image updates:**  
The Dockerfile uses `python:3.14-slim`. Add [Renovate](https://docs.renovatebot.com/) or [Dependabot](https://docs.github.com/en/code-security/dependabot) to the repository so base image upgrades (security patches) generate automatic PRs, keeping the attack surface minimal without manual tracking.
