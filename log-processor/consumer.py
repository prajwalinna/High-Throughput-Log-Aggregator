import gzip
import json
import time
import uuid

import boto3
from kafka import KafkaConsumer


BATCH_SIZE = 10  # Testing only. Change to 1000 later.
BUCKET_NAME = "log-aggregator-archive"


# --------------------------------------------------
# S3 / Floci setup
# --------------------------------------------------

s3 = boto3.client(
    "s3",
    endpoint_url="http://floci:4566",
    region_name="us-east-1",
    aws_access_key_id="test",
    aws_secret_access_key="test",
)

try:
    s3.head_bucket(Bucket=BUCKET_NAME)
    print(f"S3 bucket exists: {BUCKET_NAME}")

except Exception:
    print(f"Creating S3 bucket: {BUCKET_NAME}")
    s3.create_bucket(Bucket=BUCKET_NAME)


# --------------------------------------------------
# Kafka setup
# --------------------------------------------------

consumer = KafkaConsumer(
    "logs",
    bootstrap_servers=["kafka:9092"],
    group_id="log-processors",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda value: json.loads(
        value.decode("utf-8")
    ),
)


# --------------------------------------------------
# Upload batch to S3
# --------------------------------------------------

def upload_batch(logs):

    # Convert list of logs to NDJSON.
    data = "\n".join(
        json.dumps(log)
        for log in logs
    ).encode("utf-8")

    # Compress using gzip.
    compressed_data = gzip.compress(data)

    # Unique object name.
    timestamp = int(time.time())

    object_key = (
        f"logs/{timestamp}/"
        f"batch-{uuid.uuid4()}.json.gz"
    )

    # Upload to Floci S3.
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=object_key,
        Body=compressed_data,
        ContentType="application/x-ndjson",
        ContentEncoding="gzip",
    )

    print(
        f"Uploaded {len(logs)} logs "
        f"to s3://{BUCKET_NAME}/{object_key}"
    )


# --------------------------------------------------
# Consume Kafka
# --------------------------------------------------

print("Log processor started")
print(f"Batch size: {BATCH_SIZE}")

batch = []

for message in consumer:

    log = message.value

    # Ignore invalid messages.
    if not isinstance(log, dict):
        print("Skipping non-dictionary log")
        continue

    # Ignore malformed records like the one we saw earlier.
    if not log.get("service"):
        print("Skipping malformed log")
        continue

    batch.append(log)

    if len(batch) >= BATCH_SIZE:

        try:
            upload_batch(batch)

            # Clear only AFTER successful upload.
            batch.clear()

        except Exception as error:
            print(f"S3 upload failed: {error}")