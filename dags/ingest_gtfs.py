from datetime import datetime, timedelta
import io
import os
import zipfile

import pandas as pd
import requests
from airflow import DAG
from airflow.decorators import task
from minio import Minio
import pyarrow as pa
import pyarrow.parquet as pq

MINIO_CLIENT = Minio(
    os.getenv("MINIO_ENDPOINT", "minio:9000"),
    access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
    secure=False,
)

GTFS_FILES = [
    "agency.txt",
    "routes.txt",
    "stops.txt",
    "trips.txt",
    "stop_times.txt",
    "calendar.txt",
    "transfers.txt",  # опциональный
]

with DAG(
    dag_id="ingest_gtfs_prague",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 4 * * 1",
    catchup=False,
    default_args={
        "owner": "data",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["ingest", "gtfs", "transport"],
) as dag:

    @task
    def download_and_upload_gtfs():
        resp = requests.get("https://pid.cz/gtd/gtfs.zip", timeout=120)
        if resp.status_code != 200:
            raise Exception(f"GTFS download failed: {resp.status_code}")

        z = zipfile.ZipFile(io.BytesIO(resp.content))
        file_list = z.namelist()
        print(f"Archive contains: {file_list}")

        today = datetime.utcnow().strftime("%Y-%m-%d")
        bucket = os.getenv("MINIO_BUCKET_RAW", "europe-raw")

        for filename in GTFS_FILES:
            if filename not in file_list:
                print(f"File {filename} not in archive, skipping")
                continue

            df = pd.read_csv(z.open(filename), low_memory=False)
            table = pa.Table.from_pandas(df)
            buf = io.BytesIO()
            pq.write_table(table, buf, compression="snappy")
            buf.seek(0)

            entity_name = filename.replace(".txt", "")
            key = f"gtfs/prague/{entity_name}/{today}/data.parquet"

            MINIO_CLIENT.put_object(
                bucket, key, buf, length=buf.getbuffer().nbytes,
                content_type="application/parquet",
            )
            print(f"Uploaded {filename}: {len(df)} rows to s3://{bucket}/{key}")

    download_and_upload_gtfs()
