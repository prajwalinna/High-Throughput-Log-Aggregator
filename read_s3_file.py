import boto3
import gzip
import io
import json

def read_latest_s3_file():
    print("Connecting to local S3 (Floci)...")
    s3 = boto3.client(
        's3',
        endpoint_url='http://localhost:4566',
        aws_access_key_id='test',
        aws_secret_access_key='test',
        region_name='us-east-1'
    )
    
    bucket_name = 'log-aggregator-archive'
    try:
        response = s3.list_objects_v2(Bucket=bucket_name)
        if 'Contents' not in response:
            print("Bucket is empty!")
            return

        # Get the most recently uploaded file
        latest_file = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)[0]
        file_key = latest_file['Key']
        print(f"\nDownloading the latest file: {file_key} ({latest_file['Size']} bytes)")

        # Download the file
        obj = s3.get_object(Bucket=bucket_name, Key=file_key)
        compressed_data = obj['Body'].read()

        # Decompress and read
        print("\n--- File Contents (First 3 logs) ---")
        with gzip.GzipFile(fileobj=io.BytesIO(compressed_data)) as gz:
            lines = gz.readlines()
            for i, line in enumerate(lines[:3]):
                log_entry = json.loads(line)
                print(json.dumps(log_entry, indent=2))
            
            if len(lines) > 3:
                print(f"... and {len(lines) - 3} more logs in this batch.")

    except Exception as e:
        print(f"Error accessing S3: {e}")

if __name__ == "__main__":
    read_latest_s3_file()
