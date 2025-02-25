from io import StringIO
import pandas as pd
import numpy as np
import logging
import boto3
import time
import os

#create log file if it does not exist
data_validation_log_file = "C:\\Annie\\Trial\\logs\\data_validation.log"
logging.root.handlers = []
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO , filename=data_validation_log_file)

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
                  aws_secret_access_key="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",
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
                      aws_secret_access_key="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",
                      region_name="us-east-1"
    )
    bucket_name = 'dmmlassignmentbucket'
    s3_csv_key = f'{zone_name}/{timestr}/{folder_name}/{csv_filename}'
    s3.upload_file(csv_filename, bucket_name, s3_csv_key) 
    logging.info(f"Successfully uploaded data {s3_csv_key} to AWS S3 {bucket_name} bucket.")

def generate_csv_data_quality_report(csv_filename, output_path="csv_data_quality_report.csv"):
    
    logging.info("Starting data validation")
    timestr = time.strftime("%Y%m%d")
    df = read_csv_from_s3(csv_filename, zone_name="landing", folder_name="csv")
    report_data = []
    logging.info("Running validation on data received from S3")
    
    for col in df.columns:
        data_type = df[col].dtype
        remarks = ""        
        # checking missing empty or null data
        missing_count = df[col].isnull().sum() + df[col].isna().sum()
        
        
        missing_percentage = (missing_count / len(df)) * 100
        unique_count = df[col].nunique()
        # Numeric Columns
        if pd.api.types.is_numeric_dtype(df[col]):
            min_val = df[col].min()
            max_val = df[col].max()
            mean_val = df[col].mean()
            median_val = df[col].median()
            max_allowed_value = 100
            iqr = df[col].quantile(0.75) - df[col].quantile(0.25)
            potential_outlier1 = df[df[col] < iqr*-3.5]
            potential_outlier2 = df[df[col] > iqr*3.5]
            if not (potential_outlier1.empty & potential_outlier2.empty):
                remarks += f"{col} has {len(potential_outlier1) + len(potential_outlier2)} potential outlier rows."
            
            if col == 'tenure':
                max_allowed_value = 80  # Example
            invalid_data = df[df[col] > max_allowed_value]
            if not invalid_data.empty:
                remarks += f"{len(invalid_data)} rows have '{col}' > {max_allowed_value}."
            
                
            report_data.append([col, data_type, missing_count, missing_percentage, unique_count, min_val, max_val, mean_val, median_val, remarks])
        # Categorical/Object Columns
        elif pd.api.types.is_object_dtype(df[col]):
            empty_rows = df[col].str.strip()
            empty_rows = empty_rows.replace('', np.nan)
            missing_count = empty_rows.isnull().sum() + empty_rows.isna().sum()
            missing_percentage = (missing_count / len(df)) * 100
            report_data.append([col, data_type, missing_count, missing_percentage, unique_count, None, None, None, None, remarks])
        # Boolean Columns
        elif pd.api.types.is_bool_dtype(df[col]):
            report_data.append([col, data_type, missing_count, missing_percentage, unique_count, None, None, None, None, remarks])
        # Datetime Columns
        elif pd.api.types.is_datetime64_any_dtype(df[col]):
            report_data.append([col, data_type, missing_count, missing_percentage, unique_count, None, None, None, None, remarks])
        else:
            report_data.append([col, data_type, missing_count, missing_percentage, unique_count, None, None, None, None, remarks])

    report_df = pd.DataFrame(report_data, columns=[
        "Column", "Data Type", "Missing Count", "Missing Percentage", "Unique Count",
        "Min", "Max", "Mean", "Median", "Remarks"
    ])

    logging.info("Saving metrics to S3")

    outdir = 'reports'
    if not os.path.exists(outdir):
        os.mkdir(outdir)
    output_path = f"{outdir}/{output_path}_{timestr}.csv"
    report_df.to_csv(output_path, index=False)
    save_csv_to_s3(output_path, "raw", "csv")
    logging.info(f"Metrics saved to: {output_path}")
    
    df.to_csv(csv_filename, index=False)
    save_csv_to_s3(csv_filename, "raw", "csv")
    logging.info("Saving csv as trusted")
    
    os.remove(output_path)


generate_csv_data_quality_report("Telco-Customer-Churn.csv")