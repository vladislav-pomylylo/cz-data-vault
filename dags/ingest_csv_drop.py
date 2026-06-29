from datetime import datetime, timedelta
import io
import json
import os
import csv

import pandas as pd
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

LANDING_DIR = "/opt/airflow/data/landing"
BUCKET = os.getenv("MINIO_BUCKET_RAW", "europe-raw")
REQUIRED_COLUMNS = [
    "sale_date", "country_code", "city", "product_category",
    "product_name", "quantity", "unit_price", "currency",
]


def get_tracker() -> list[str]:
    try:
        resp = MINIO_CLIENT.get_object(BUCKET, "csv_drop/.tracker.json")
        return json.loads(resp.read().decode("utf-8"))
    except Exception:
        return []


def save_tracker(files: list[str]):
    buf = io.BytesIO(json.dumps(files).encode("utf-8"))
    MINIO_CLIENT.put_object(
        BUCKET, "csv_drop/.tracker.json", buf,
        length=buf.getbuffer().nbytes,
        content_type="application/json",
    )


def validate_csv(filepath: str) -> bool:
    try:
        with open(filepath, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)
            header_stripped = [c.strip() for c in header]
            for col in REQUIRED_COLUMNS:
                if col not in header_stripped:
                    print(f"Missing column '{col}' in {filepath}")
                    return False
        return True
    except Exception as e:
        print(f"CSV validation failed for {filepath}: {e}")
        return False


with DAG(
    dag_id="ingest_csv_drop",
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/15 * * * *",
    catchup=False,
    default_args={
        "owner": "data",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["ingest", "csv", "sales"],
) as dag:

    @task
    def process_new_files():
        tracker = set(get_tracker())
        landing_files = [
            f for f in os.listdir(LANDING_DIR)
            if f.endswith(".csv") and f not in tracker
        ]

        if not landing_files:
            print("No new files to process")
            return

        for filename in sorted(landing_files):
            filepath = os.path.join(LANDING_DIR, filename)
            print(f"Processing {filename}...")

            if not validate_csv(filepath):
                print(f"Skipping invalid file: {filename}")
                tracker.add(filename)
                save_tracker(list(tracker))
                continue

            try:
                df = pd.read_csv(filepath)
                df["_filename"] = filename
                df["_ingested_at"] = datetime.utcnow().isoformat()

                table = pa.Table.from_pandas(df)
                buf = io.BytesIO()
                pq.write_table(table, buf, compression="snappy")
                buf.seek(0)

                today = datetime.utcnow().strftime("%Y-%m-%d")
                key = f"csv_sales/{today}/{filename.replace('.csv', '.parquet')}"

                MINIO_CLIENT.put_object(
                    BUCKET, key, buf, length=buf.getbuffer().nbytes,
                    content_type="application/parquet",
                )
                print(f"Uploaded {len(df)} rows to s3://{BUCKET}/{key}")

            except Exception as e:
                print(f"Error processing {filename}: {e}")
                tracker.add(filename)
                save_tracker(list(tracker))
                raise

            tracker.add(filename)
            save_tracker(list(tracker))

    process_new_files()
