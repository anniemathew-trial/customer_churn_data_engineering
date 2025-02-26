import boto3

s3 = boto3.client("s3",
                      aws_access_key_id="AKIA34AMCY3VJO7OWYOX",
                      aws_secret_access_key="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",
                      region_name="us-east-1"
    )
bucket_name = 'dmmlassignmentbucket'
s3_csv_key = "test_upload.txt" 
try:
    s3.put_object(Bucket=bucket_name, Key=s3_csv_key, Body="Hello ,S3!")
    print("Access works")
except Exception as e:
    print(f'Access does not work: {e}')