from sklearn.preprocessing import StandardScaler
from io import StringIO
import pandas as pd
import logging
import boto3
import time
#create log file if it does not exist
data_transformation_and_storage_log_file = "C:\\Annie\\Trial\\logs\\data_transformation_and_storage.log"
logging.root.handlers = []
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO , filename=data_transformation_and_storage_log_file)

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

def save_to_s3(filename, zone_name, folder_name):
    timestr = time.strftime("%Y%m%d")
    logging.info("Logging in to AWS S3")
    s3 = boto3.client("s3",
                      aws_access_key_id="AKIA34AMCY3VJO7OWYOX",
                      #aws_secret_access_key="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",
                      region_name="us-east-1"
    )
    bucket_name = 'dmmlassignmentbucket'
    s3_csv_key = f'{zone_name}/{timestr}/{folder_name}/{filename}'
    s3.upload_file(filename, bucket_name, s3_csv_key) 
    logging.info(f"Successfully uploaded data {s3_csv_key} to AWS S3 {bucket_name} bucket.")

def data_transformation():
    logging.info("Starting data preparation for csv.")
    # Read data from Amazon S3 bucket
    df = read_csv_from_s3('Telco-Customer-Churn.csv',zone_name="raw", folder_name="csv")
#convert yes/no to True/False
    yes_no_columns = ["SeniorCitizen","Dependents", "PhoneService","OnlineSecurity", "OnlineBackup", "DeviceProtection", "TechSupport", "StreamingTV", "StreamingMovies","PaperlessBilling", "Churn"]
    for col in yes_no_columns:
        df[col] = df[col].replace({"Yes":True, "No":False})
    df[yes_no_columns] = df[yes_no_columns].astype("bool")
    df["MultipleLines"] = df["MultipleLines"].astype("str")

    #One-Hot encoding for Categorical Variables
    df = pd.get_dummies(df, columns=["Contract","PaymentMethod"], drop_first=True)

    #Feature creation
    df["TenureGroup"] = pd.cut(df["tenure"], bins=[0,12,24,48,60,100], labels=["0-1 Year", "1-2 Years", "2-4 Years", "4-5 Years", "5+ Years"])
    df = pd.get_dummies(df, columns=["TenureGroup"], drop_first=True)
    df["ChargeRatio"] = df["TotalCharges"]/(df["MonthlyCharges"]+1)
    df["Tenure_MonthlyCharge"] = df["tenure"] * df["MonthlyCharges"]

    scaler = StandardScaler()
    df[['tenure', 'MonthlyCharges', 'TotalCharges']] = scaler.fit_transform(df[['tenure', 'MonthlyCharges', 'TotalCharges']])