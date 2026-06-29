import os
from minio import Minio

client = Minio(
    os.getenv("MINIO_ENDPOINT", "localhost:9000"),
    access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
    secure=False,
)

buckets = [
    os.getenv("MINIO_BUCKET_RAW", "europe-raw"),
    os.getenv("MINIO_BUCKET_LAKE", "europe-lake"),
]

for bucket in buckets:
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        print(f"Bucket '{bucket}' created")
    else:
        print(f"Bucket '{bucket}' already exists")
