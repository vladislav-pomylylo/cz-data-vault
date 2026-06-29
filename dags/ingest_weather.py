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

CITIES = [
    {"city": "Prague", "lat": 50.08, "lon": 14.44, "country": "CZ"},
    {"city": "Vienna", "lat": 48.21, "lon": 16.37, "country": "AT"},
    {"city": "Berlin", "lat": 52.52, "lon": 13.41, "country": "DE"},
    {"city": "Warsaw", "lat": 52.24, "lon": 21.01, "country": "PL"},
    {"city": "Bratislava", "lat": 48.15, "lon": 17.11, "country": "SK"},
    {"city": "Budapest", "lat": 47.50, "lon": 19.04, "country": "HU"},
    {"city": "Paris", "lat": 48.86, "lon": 2.35, "country": "FR"},
    {"city": "Madrid", "lat": 40.42, "lon": -3.70, "country": "ES"},
    {"city": "Rome", "lat": 41.90, "lon": 12.50, "country": "IT"},
    {"city": "Amsterdam", "lat": 52.37, "lon": 4.89, "country": "NL"},
    {"city": "Brussels", "lat": 50.85, "lon": 4.35, "country": "BE"},
    {"city": "Copenhagen", "lat": 55.68, "lon": 12.57, "country": "DK"},
    {"city": "Stockholm", "lat": 59.33, "lon": 18.07, "country": "SE"},
    {"city": "Helsinki", "lat": 60.17, "lon": 24.94, "country": "FI"},
    {"city": "Lisbon", "lat": 38.72, "lon": -9.13, "country": "PT"},
    {"city": "Dublin", "lat": 53.35, "lon": -6.26, "country": "IE"},
]

with DAG(
    dag_id="ingest_weather",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 2 * * *",
    catchup=False,
    default_args={
        "owner": "data",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["ingest", "weather"],
) as dag:

    @task
    def fetch_weather():
        rows = []
        for city in CITIES:
            url = (
                f"https://api.open-meteo.com/v1/forecast"
                f"?latitude={city['lat']}&longitude={city['lon']}"
                f"&current=temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m"
                f"&timezone=auto"
            )
            try:
                resp = requests.get(url, timeout=15)
                if resp.status_code != 200:
                    print(f"Failed for {city['city']}: {resp.status_code}")
                    continue
                data = resp.json()
                current = data.get("current", {})
                rows.append({
                    "city": city["city"],
                    "country": city["country"],
                    "lat": city["lat"],
                    "lon": city["lon"],
                    "timestamp": current.get("time", datetime.utcnow().isoformat()),
                    "temperature_c": current.get("temperature_2m"),
                    "humidity_pct": current.get("relative_humidity_2m"),
                    "precipitation_mm": current.get("precipitation"),
                    "wind_speed_kmh": current.get("wind_speed_10m"),
                })
                print(f"Fetched weather for {city['city']}: {current.get('temperature_2m')}°C")
            except Exception as e:
                print(f"Error fetching {city['city']}: {e}")
                continue

        df = pd.DataFrame(rows)
        print(f"Total cities fetched: {len(df)}")

        table = pa.Table.from_pandas(df)
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        buf.seek(0)

        today = datetime.utcnow().strftime("%Y-%m-%d")
        key = f"weather/daily/{today}/data.parquet"

        bucket = os.getenv("MINIO_BUCKET_RAW", "europe-raw")
        MINIO_CLIENT.put_object(
            bucket, key, buf, length=buf.getbuffer().nbytes,
            content_type="application/parquet",
        )
        print(f"Uploaded weather data: {len(df)} cities to s3://{bucket}/{key}")

    fetch_weather()
