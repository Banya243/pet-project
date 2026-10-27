"""Create the lab buckets and seed input without overwriting learner data."""

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

client = boto3.client(
    "s3",
    endpoint_url="http://minio:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
    region_name="us-east-1",
    config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
)

for bucket in ("raw", "processed"):
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError as error:
        if error.response["ResponseMetadata"]["HTTPStatusCode"] != 404:
            raise
        client.create_bucket(Bucket=bucket)

key = "orders/2026-10-26/orders.csv"
try:
    client.head_object(Bucket="raw", Key=key)
except ClientError as error:
    if error.response["ResponseMetadata"]["HTTPStatusCode"] != 404:
        raise
    client.upload_file("/seed/orders.csv", "raw", key)

print(f"S3 ready: raw, processed; input s3://raw/{key}")
