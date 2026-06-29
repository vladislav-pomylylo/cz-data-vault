from datetime import datetime, timedelta
import io
import os

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

DATASETS = {
    "prc_hicp_manr": "hicp_inflation",
    "nama_10_gdp": "regional_gdp",
    "demo_pjan": "population",
}

def fetch_eurostat_tsv(dataset_code: str) -> pd.DataFrame:
    url = f"https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/{dataset_code}?format=TSV"
    resp = requests.get(url, timeout=60)
    if resp.status_code != 200:
        raise Exception(f"Eurostat error {resp.status_code} for {dataset_code}")

    df = pd.read_csv(
        io.StringIO(resp.text),
        sep="\t",
        low_memory=False,
    )
    print(f"Dataset {dataset_code}: {len(df)} rows, {len(df.columns)} columns")
    return df

with DAG(
    dag_id="ingest_eurostat",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 8 2 * *",
    catchup=False,
    default_args={
        "owner": "data",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["ingest", "eurostat", "economy"],
) as dag:

    @task
    def fetch_all_datasets():
        for code, name in DATASETS.items():
            try:
                df = fetch_eurostat_tsv(code)
            except Exception as e:
                print(f"Failed to fetch {code}: {e}")
                continue

            if df.empty:
                print(f"No data for {code}")
                continue

            table = pa.Table.from_pandas(df)
            buf = io.BytesIO()
            pq.write_table(table, buf, compression="snappy")
            buf.seek(0)

            today = datetime.utcnow().strftime("%Y-%m-%d")
            key = f"eurostat/{name}/{today}/data.parquet"

            bucket = os.getenv("MINIO_BUCKET_RAW", "europe-raw")
            MINIO_CLIENT.put_object(
                bucket, key, buf, length=buf.getbuffer().nbytes,
                content_type="application/parquet",
            )
            print(f"Uploaded {len(df)} rows to s3://{bucket}/{key}")

    fetch_all_datasets()
