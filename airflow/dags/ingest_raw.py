from datetime import datetime

import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.http.sensors.http import HttpSensor


SOURCE_URL = (
    "https://archive.ics.uci.edu/"
    "ml/machine-learning-databases/00560/SeoulBikeData.csv"
)

SOURCE_ENDPOINT = (
    "/ml/machine-learning-databases/00560/SeoulBikeData.csv"
)

FILE_NAME = "SeoulBikeData.csv"
DATASET_SLUG = "seoul-bike-sharing-demand"

RAW_BUCKET = "raw"

S3_CONN_ID = "s3_conn"


def load_to_raw(
    source_url: str,
    file_name: str,
    dataset_slug: str,
    s3_conn_id: str,
    **context,
):
    logical_date = context["ds"]

    object_key = (
        f"{dataset_slug}/"
        f"ingested_on={logical_date}/"
        f"{file_name}"
    )

    s3_hook = S3Hook(
        aws_conn_id=s3_conn_id,
    )

    # Создаём бакет raw при первой загрузке.
    if not s3_hook.check_for_bucket(RAW_BUCKET):
        s3_hook.create_bucket(
            bucket_name=RAW_BUCKET,
        )

    # Если файл за эту логическую дату уже существует,
    # повторно его не загружаем и не изменяем.
    if s3_hook.check_for_key(
        key=object_key,
        bucket_name=RAW_BUCKET,
    ):
        print(
            f"Object already exists: "
            f"s3://{RAW_BUCKET}/{object_key}. "
            f"Skipping upload."
        )
        return

    # Получаем исходный файл по HTTP.
    response = requests.get(
        source_url,
        timeout=60,
    )

    response.raise_for_status()

    # Загружаем исходные байты без преобразований.
    s3_hook.load_bytes(
        bytes_data=response.content,
        key=object_key,
        bucket_name=RAW_BUCKET,
        replace=False,
    )

    print(
        f"Uploaded successfully: "
        f"s3://{RAW_BUCKET}/{object_key}"
    )


with DAG(
    dag_id="ingest_raw",
    start_date=datetime(2026, 9, 1),
    schedule=None,
    catchup=False,
    tags=["lab-02"],
) as dag:

    wait_for_primary_source = HttpSensor(
        task_id="wait_for_primary_source",
        http_conn_id="source_http_conn",
        endpoint=SOURCE_ENDPOINT,
        method="GET",
        poke_interval=60,
        timeout=600,
    )

    load_raw = PythonOperator(
        task_id="load_to_raw",
        python_callable=load_to_raw,
        op_kwargs={
            "source_url": SOURCE_URL,
            "file_name": FILE_NAME,
            "dataset_slug": DATASET_SLUG,
            "s3_conn_id": S3_CONN_ID,
        },
    )

    wait_for_primary_source >> load_raw
