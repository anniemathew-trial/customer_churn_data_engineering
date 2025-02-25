from io import StringIO
import pandas as pd
import logging
import boto3
import time

#create log file if it does not exist
raw_data_log_file = "C:\\Annie\\Trial\\logs\\raw_data_storage.log"
logging.root.handlers = []
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO , filename=raw_data_log_file)

# set up logging
console = logging.StreamHandler()
console.setLevel(logging.ERROR)

# set a format which is simpler for console use
formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(message)s')
console.setFormatter(formatter)
logging.getLogger("").addHandler(console)

def read_csv_from_s3(csv_filename, zone_name, folder_name):
    timestr = time.strftime("%Y%m%d")
    logging.info("Logging in to AWS S3")
    s3 = boto3.client("s3",
                  aws_access_key_id="AKIA34AMCY3VJO7OWYOX",
                  #aws_secret_access_key="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",
                  region_name="us-east-1"
    )
    bucket_name = 'dmmlassignmentbucket'
    s3_csv_key = f'{zone_name}/{timestr}/{folder_name}/{csv_filename}'
    logging.info(f"Getting {s3_csv_key} from AWS S3")
    response = s3.get_object(Bucket=bucket_name, Key=s3_csv_key)
    csv_content = response["Body"].read().decode("utf-8")
    df = pd.read_csv(StringIO(csv_content))
    logging.info(f"Successfully retrieved data {s3_csv_key} from AWS S3 {bucket_name} bucket.")
    return df;

def save_csv_to_s3(csv_filename, zone_name, folder_name):
    timestr = time.strftime("%Y%m%d")
    logging.info("Logging in to AWS S3")
    s3 = boto3.client("s3",
                      aws_access_key_id="AKIA34AMCY3VJO7OWYOX",
                      #aws_secret_access_key="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",
                      region_name="us-east-1"
    )
    bucket_name = 'dmmlassignmentbucket'
    s3_csv_key = f'{zone_name}/{timestr}/{folder_name}/{csv_filename}'
    s3.upload_file(csv_filename, bucket_name, s3_csv_key) 
    logging.info(f"Successfully uploaded data {s3_csv_key} to AWS S3 {bucket_name} bucket.")
    
    
save_csv_to_s3("Telco-Customer-Churn.csv", "landing", "csv")
save_csv_to_s3("database_data.csv", "landing", "db")