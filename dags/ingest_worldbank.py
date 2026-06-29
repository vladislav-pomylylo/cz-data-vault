from datetime import datetime, timedelta
import os
import io

import pandas as pd
import requests
from airflow import DAG
from airflow.decorators import task
from minio import Minio
import pyarrow as pa
import pyarrow.parquet as pq

COUNTRIES = [
    "CZ", "AT", "DE", "PL", "SK", "HU", "FR", "ES", "IT",
    "NL", "BE", "DK", "SE", "FI", "PT", "IE", "RO", "BG",
    "HR", "SI", "LT", "LV", "EE",
]

INDICATORS = {
    "NY.GDP.MKTP.CD": "gdp_usd",
    "NY.GDP.MKTP.KD.ZG": "gdp_growth_pct",
    "FP.CPI.TOTL.ZG": "inflation_pct",
    "SL.UEM.TOTL.ZS": "unemployment_pct",
    "SP.POP.TOTL": "population",
}

MINIO_CLIENT = Minio(
    os.getenv("MINIO_ENDPOINT", "minio:9000"),
    access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
    secure=False,
)

def fetch_worldbank(country_code: str, indicator: str) -> list[dict]:
    url = (
        f"https://api.worldbank.org/v2/country/{country_code}"
        f"/indicator/{indicator}?format=json&per_page=1000"
    )
    resp = requests.get(url, timeout=30)
    if resp.status_code != 200:
        raise Exception(f"WorldBank API error {resp.status_code}: {resp.text}")
    data = resp.json()
    if not data or len(data) < 2:
        print(f"No data for {country_code}/{indicator}")
        return []
    records = []
    for item in data[1]:
        if item.get("value") is not None:
            records.append({
                "country_code": country_code,
                "year": int(item["date"]),
                "indicator": indicator,
                "indicator_name": INDICATORS[indicator],
                "value": float(item["value"]),
            })
    return records

with DAG(
    dag_id="ingest_worldbank",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 6 1 * *",
    catchup=False,
    default_args={
        "owner": "data",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
    },
    tags=["ingest", "worldbank", "economy"],
) as dag:

    @task
    def fetch_all_indicators():
        all_records = []
        for country in COUNTRIES:
            for indicator in INDICATORS:
                records = fetch_worldbank(country, indicator)
                all_records.extend(records)
                print(f"Country {country}, indicator {indicator}: {len(records)} records")
        print(f"Total records fetched: {len(all_records)}")
        return all_records

    @task
    def upload_to_minio(records: list[dict]):
        if not records:
            print("No records to upload")
            return

        df = pd.DataFrame(records)
        table = pa.Table.from_pandas(df)

        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        buf.seek(0)

        today = datetime.utcnow().strftime("%Y-%m-%d")
        key = f"worldbank/{today}/data.parquet"

        bucket = os.getenv("MINIO_BUCKET_RAW", "europe-raw")
        MINIO_CLIENT.put_object(
            bucket, key, buf, length=buf.getbuffer().nbytes,
            content_type="application/parquet",
        )
        print(f"Uploaded {len(df)} rows to s3://{bucket}/{key}")
        print(f"File size: {buf.getbuffer().nbytes / 1024:.2f} KB")

    upload_to_minio(fetch_all_indicators())
