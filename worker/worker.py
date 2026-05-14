"""
ML Demo worker application
---------------------
* Read a YAML job configuration
* pull an input CSV from RustFS
* runs a mocked job
* writes the resulting .npy file back to a target RustFS bucket
"""

import io
import logging
import os
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import yaml
from minio import Minio # RustFS is backaward compatible with Minio API
from minio.error import S3Error

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("ml-worker")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

REQUIRED_CONFIG_KEYS = {"job_id", "batch_size", "model_name", "input_csv"}


def load_job_config(config_path: str) -> dict:
    """Parse and validate the YAML job configuration file."""
    logger.info("Loading job configuration from '%s'", config_path)
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with path.open("r") as fh:
        config = yaml.safe_load(fh)

    if not isinstance(config, dict):
        raise ValueError("Job config must be a YAML mapping at the top level.")

    missing = REQUIRED_CONFIG_KEYS - config.keys()
    if missing:
        raise KeyError(f"Job config is missing required keys: {missing}")

    logger.info(
        "Job config loaded | job_id=%s model=%s batch_size=%s input_csv=%s",
        config["job_id"],
        config["model_name"],
        config["batch_size"],
        config["input_csv"],
    )
    return config


# ---------------------------------------------------------------------------
# MinIO helpers
# ---------------------------------------------------------------------------

def build_minio_client() -> Minio:
    """Construct a MinIO client from environment variables."""
    endpoint = os.environ.get("S3_ENDPOINT")
    access_key = os.environ.get("S3_ACCESS_KEY")
    secret_key = os.environ.get("S3_SECRET_KEY")
    use_tls = os.environ.get("S3_USE_TLS", "false").lower() == "true"

    if not all([endpoint, access_key, secret_key]):
        raise EnvironmentError(
            "S3_ENDPOINT, S3_ACCESS_KEY, and S3_SECRET_KEY must all be set."
        )

    logger.info(
        "Connecting to S3 Backend | endpoint=%s tls=%s", endpoint, use_tls
    )
    client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=use_tls)
    return client


def ensure_bucket(client: Minio, bucket: str) -> None:
    """Create the bucket if it does not already exist."""
    if not client.bucket_exists(bucket):
        logger.info("Bucket '%s' does not exist – creating it.", bucket)
        client.make_bucket(bucket)
    else:
        logger.debug("Bucket '%s' already exists.", bucket)


def download_csv(client: Minio, bucket: str, object_name: str) -> pd.DataFrame:
    """Stream a CSV object from MinIO into a Pandas DataFrame."""
    logger.info("Downloading CSV | bucket=%s object=%s", bucket, object_name)
    try:
        response = client.get_object(bucket, object_name)
        data = response.read()
        response.close()
        response.release_conn()
    except S3Error as exc:
        logger.error(
            "Failed to download object | bucket=%s object=%s error=%s",
            bucket, object_name, exc,
        )
        raise

    df = pd.read_csv(io.BytesIO(data))
    logger.info(
        "CSV downloaded successfully | rows=%d columns=%d", len(df), len(df.columns)
    )
    return df


def upload_npy(client: Minio, bucket: str, object_name: str, array: np.ndarray) -> None:
    """Serialise a NumPy array and upload it to MinIO."""
    logger.info(
        "Uploading .npy file | bucket=%s object=%s shape=%s dtype=%s",
        bucket, object_name, array.shape, array.dtype,
    )
    buffer = io.BytesIO()
    np.save(buffer, array)
    buffer_size = buffer.tell()
    buffer.seek(0)

    try:
        client.put_object(
            bucket,
            object_name,
            data=buffer,
            length=buffer_size,
            content_type="application/octet-stream",
        )
    except S3Error as exc:
        logger.error(
            "Failed to upload object | bucket=%s object=%s error=%s",
            bucket, object_name, exc,
        )
        raise

    logger.info(
        "Upload complete | bucket=%s object=%s bytes=%d",
        bucket, object_name, buffer_size,
    )


# ---------------------------------------------------------------------------
# Mocked inference
# ---------------------------------------------------------------------------

def mocked_inference(df: pd.DataFrame, model_name: str, batch_size: int) -> np.ndarray:
    """
    Mocked ML job

    Args:
        df:         Input data loaded from the CSV.
        model_name: Name of the (mocked) model – used only for logging.
        batch_size: Batch size – used only for logging/simulated timing.

    Returns:
        NumPy array of shape (n_rows, 512).
    """
    n_rows = len(df)
    embedding_dim = 512
    n_batches = max(1, -(-n_rows // batch_size))  # ceiling division

    logger.info(
        "Starting mocked inference | model=%s n_rows=%d batch_size=%d n_batches=%d embedding_dim=%d",
        model_name, n_rows, batch_size, n_batches, embedding_dim,
    )

    rng = np.random.default_rng(seed=42)
    results = []

    for batch_idx in range(n_batches):
        start = batch_idx * batch_size
        end = min(start + batch_size, n_rows)
        current_batch_size = end - start

        logger.debug(
            "Processing batch %d/%d | rows %d–%d",
            batch_idx + 1, n_batches, start, end - 1,
        )

        # Simulate per-batch latency
        time.sleep(0.05)

        batch_output = rng.random((current_batch_size, embedding_dim), dtype=np.float32)
        results.append(batch_output)

    embeddings = np.vstack(results)
    logger.info(
        "Mocked inference complete | output_shape=%s", embeddings.shape
    )
    return embeddings


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def run(config_path: str) -> None:
    """End-to-end worker pipeline."""
    start_time = time.monotonic()
    logger.info("=== ML Worker starting ===")

    # 1. Load configuration
    config = load_job_config(config_path)
    job_id = config["job_id"]
    model_name = config["model_name"]
    batch_size = int(config["batch_size"])
    input_csv = config["input_csv"]  # e.g. "my-bucket/path/to/data.csv"

    # Derive source bucket / object from the input_csv value.
    # Expected format: "<bucket>/<object_key>"
    if "/" not in input_csv:
        raise ValueError(
            f"'input_csv' must be in the form '<bucket>/<object_key>', got: {input_csv!r}"
        )
    source_bucket, source_object = input_csv.split("/", 1)

    output_bucket = config.get("output_bucket", "ml-outputs")
    output_object = config.get(
        "output_object", f"jobs/{job_id}/embeddings.npy"
    )

    # 2. Build MinIO client
    minio_client = build_minio_client()

    # 3. Ensure output bucket exists
    ensure_bucket(minio_client, output_bucket)

    # 4. Download input CSV
    df = download_csv(minio_client, source_bucket, source_object)

    # 5. Run mocked inference
    embeddings = mocked_inference(df, model_name, batch_size)

    # 6. Upload result
    upload_npy(minio_client, output_bucket, output_object, embeddings)

    elapsed = time.monotonic() - start_time
    logger.info(
        "=== ML Worker finished | job_id=%s elapsed_seconds=%.2f ===",
        job_id, elapsed,
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    config_file = os.environ.get("JOB_CONFIG_PATH", "/app/config/job_config.yaml")
    try:
        run(config_file)
    except Exception as exc:
        logger.exception("Worker failed with an unhandled exception: %s", exc)
        sys.exit(1)

