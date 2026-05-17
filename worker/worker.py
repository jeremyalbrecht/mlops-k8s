import io
import logging
import os
import sys
import time
from datetime import datetime
import random

import numpy as np
import pandas as pd
import yaml
from minio import Minio
from minio.error import S3Error

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("ml-worker")


def load_config(path: str) -> dict:
    with open(path) as fh:
        config = yaml.safe_load(fh)
    required = {"job_id", "batch_size", "model_name", "input_csv", "output_bucket"}
    missing = required - config.keys()
    if missing:
        raise KeyError(f"Job config missing required keys: {missing}")
    return config


def build_minio_client() -> Minio:
    endpoint   = os.environ["S3_ENDPOINT"]
    access_key = os.environ["S3_ACCESS_KEY"]
    secret_key = os.environ["S3_SECRET_KEY"]
    use_tls    = os.environ.get("S3_USE_TLS", "false").lower() == "true"
    return Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=use_tls)


def ensure_bucket(client: Minio, bucket: str) -> None:
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)


def download_csv(client: Minio, bucket: str, object_name: str) -> pd.DataFrame:
    response = client.get_object(bucket, object_name)
    data = response.read()
    response.close()
    response.release_conn()
    return pd.read_csv(io.BytesIO(data))


def upload_npy(client: Minio, bucket: str, object_name: str, array: np.ndarray) -> None:
    buffer = io.BytesIO()
    np.save(buffer, array)
    size = buffer.tell()
    buffer.seek(0)
    client.put_object(bucket, object_name, data=buffer, length=size,
                      content_type="application/octet-stream")


def mocked_inference(df: pd.DataFrame, model_name: str, batch_size: int) -> np.ndarray:
    n_rows = len(df)
    n_batches = max(1, -(-n_rows // batch_size))
    rng = np.random.default_rng(seed=42)
    results = []
    for i in range(n_batches):
        start, end = i * batch_size, min((i + 1) * batch_size, n_rows)
        time.sleep(0.05)
        results.append(rng.random((end - start, 512), dtype=np.float32))
    return np.vstack(results)


def run() -> None:
    if random.rangrange(0,10) < 5:
        print("Simulating delay in execution to trigger SLA and Deadline alerts")
        time.sleep(60)
    config = load_config(os.environ.get("JOB_CONFIG_PATH", "/app/config/job_config.yaml"))

    job_id        = config["job_id"]
    model_name    = config["model_name"]
    batch_size    = int(config["batch_size"])
    output_bucket = config["output_bucket"]

    source_bucket, source_object = config["input_csv"].split("/", 1)
    run_ts = datetime.now().strftime("%d%m%Y%H%M")
    output_object = f"jobs/{job_id}/{run_ts}/embeddings.npy"

    client = build_minio_client()
    ensure_bucket(client, output_bucket)
    df = download_csv(client, source_bucket, source_object)
    embeddings = mocked_inference(df, model_name, batch_size)
    upload_npy(client, output_bucket, output_object, embeddings)
    logger.info("job_id=%s done | output=%s/%s", job_id, output_bucket, output_object)


if __name__ == "__main__":
    try:
        run()
    except Exception as exc:
        logger.exception("Worker failed: %s", exc)
        sys.exit(1)
