import boto3

def list_s3_contents():
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
        if 'Contents' in response:
            print(f"\nBucket '{bucket_name}' contains {len(response['Contents'])} files:")
            for obj in response['Contents']:
                print(f" 📄 {obj['Key']} ({obj['Size']} bytes)")
        else:
            print(f"\nBucket '{bucket_name}' is empty.")
    except Exception as e:
        print(f"Error accessing S3: {e}")

if __name__ == "__main__":
    list_s3_contents()
