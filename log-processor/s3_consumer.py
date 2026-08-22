import gzip
import json
import time
import uuid
import boto3
from kafka import KafkaConsumer

BATCH_SIZE = 10
BUCKET_NAME = "log-aggregator-archive"

s3 = boto3.client(
    "s3",
    endpoint_url="http://floci:4566",
    region_name="us-east-1",
    aws_access_key_id="test",
    aws_secret_access_key="test",
)

def ensure_bucket_exists():
    # Keep retrying until Floci is up
    while True:
        try:
            s3.head_bucket(Bucket=BUCKET_NAME)
            print(f"S3 bucket exists: {BUCKET_NAME}")
            break
        except Exception as e:
            if "Not Found" in str(e) or "404" in str(e):
                print(f"Creating S3 bucket: {BUCKET_NAME}")
                s3.create_bucket(Bucket=BUCKET_NAME)
                break
            print(f"Waiting for Floci S3 to be ready: {e}")
            time.sleep(3)

def upload_batch(logs):
    data = "\n".join(json.dumps(log) for log in logs).encode("utf-8")
    compressed_data = gzip.compress(data)
    timestamp = int(time.time())
    object_key = f"logs/{timestamp}/batch-{uuid.uuid4()}.json.gz"

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=object_key,
        Body=compressed_data,
        ContentType="application/x-ndjson",
        ContentEncoding="gzip",
    )
    print(f"Uploaded {len(logs)} logs to s3://{BUCKET_NAME}/{object_key}")

def main():
    print("S3 Storage Consumer started")
    ensure_bucket_exists()
    
    consumer = None
    while not consumer:
        try:
            consumer = KafkaConsumer(
                "logs",
                bootstrap_servers=["kafka:9094"],
                group_id="s3-storage-group",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda value: json.loads(value.decode("utf-8"))
            )
        except Exception as e:
            print(f"Waiting for Kafka to be ready: {e}")
            time.sleep(3)
            
    print("Successfully connected to Kafka. Waiting for logs...")

    batch = []
    for message in consumer:
        log = message.value
        if not isinstance(log, dict) or not log.get("service"):
            continue

        batch.append(log)
        if len(batch) >= BATCH_SIZE:
            try:
                upload_batch(batch)
                batch.clear()
            except Exception as e:
                print(f"S3 upload failed: {e}")
                # Keep batch to retry next time

if __name__ == "__main__":
    main()
