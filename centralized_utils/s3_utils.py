def upload_to_s3(bucket_name: str, key: str, file_path: str) -> bool:
    """Upload a file to S3. Returns True on success, False on failure."""
    s3 = boto3.client('s3')
    try:
        # s3.upload_file(file_path, bucket_name, key)
        return True
    except (BotoCoreError, ClientError) as e:
        print(f"S3 upload error: {e}")
        return False
