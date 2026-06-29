import os
import requests

TRINO_URL = f"http://{os.getenv('TRINO_HOST', 'localhost')}:{os.getenv('TRINO_PORT', '8080')}"
S3_BUCKET = os.getenv("MINIO_BUCKET_RAW", "europe-raw")
S3_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")

TABLES = [
    {
        "catalog": "minio",
        "schema": "raw",
        "table": "worldbank",
        "columns": """
            country_code VARCHAR,
            year INTEGER,
            indicator VARCHAR,
            indicator_name VARCHAR,
            value DOUBLE
        """,
        "location": f"s3a://{S3_BUCKET}/worldbank",
    },
    {
        "catalog": "minio",
        "schema": "raw",
        "table": "eurostat",
        "columns": """
            dataset VARCHAR,
            geo VARCHAR,
            time VARCHAR,
            value DOUBLE
        """,
        "location": f"s3a://{S3_BUCKET}/eurostat",
    },
    {
        "catalog": "minio",
        "schema": "raw",
        "table": "gtfs_stops",
        "columns": """
            stop_id VARCHAR,
            stop_name VARCHAR,
            stop_lat DOUBLE,
            stop_lon DOUBLE,
            stop_code VARCHAR,
            zone_id VARCHAR
        """,
        "location": f"s3a://{S3_BUCKET}/gtfs/prague/stops",
    },
    {
        "catalog": "minio",
        "schema": "raw",
        "table": "gtfs_routes",
        "columns": """
            route_id VARCHAR,
            route_short_name VARCHAR,
            route_long_name VARCHAR,
            route_type INTEGER
        """,
        "location": f"s3a://{S3_BUCKET}/gtfs/prague/routes",
    },
    {
        "catalog": "minio",
        "schema": "raw",
        "table": "weather",
        "columns": """
            city VARCHAR,
            country VARCHAR,
            timestamp TIMESTAMP,
            temperature_c DOUBLE,
            humidity_pct DOUBLE,
            precipitation_mm DOUBLE,
            wind_speed_kmh DOUBLE
        """,
        "location": f"s3a://{S3_BUCKET}/weather/daily",
    },
    {
        "catalog": "minio",
        "schema": "raw",
        "table": "csv_sales",
        "columns": """
            sale_date DATE,
            country_code VARCHAR,
            city VARCHAR,
            product_category VARCHAR,
            product_name VARCHAR,
            quantity INTEGER,
            unit_price DOUBLE,
            currency VARCHAR,
            _filename VARCHAR,
            _ingested_at TIMESTAMP
        """,
        "location": f"s3a://{S3_BUCKET}/csv_sales",
    },
]


def execute_trino(sql: str):
    url = f"{TRINO_URL}/v1/statement"
    resp = requests.post(url, data=sql, headers={"X-Trino-User": "admin"})
    if resp.status_code != 200:
        print(f"Error: {resp.status_code} - {resp.text}")
        return False
    return True


def main():
    for table in TABLES:
        # Create schema if not exists
        execute_trino(f"CREATE SCHEMA IF NOT EXISTS {table['catalog']}.{table['schema']}")

        # Create table
        sql = f"""
        CREATE TABLE IF NOT EXISTS {table['catalog']}.{table['schema']}.{table['table']} (
            {table['columns']}
        ) WITH (
            format = 'PARQUET',
            external_location = '{table['location']}'
        )
        """
        if execute_trino(sql):
            print(f"Table {table['catalog']}.{table['schema']}.{table['table']} ready")


if __name__ == "__main__":
    main()
