from sklearn.preprocessing import StandardScaler
import pandas as pd
import logging
import os
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

def data_transformation(output_path="customer_data.csv"):
    logging.info("Starting data preparation for csv.")
    # Read data from Amazon S3 bucket
    df = pd.read_csv('data/cleaned/customer_data.csv')
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
    
    outdir = 'transformed'
    if not os.path.exists(outdir):
        os.mkdir(outdir)
        
    logging.info("Saving data to S3.")
    df.to_csv(f"{outdir}/{output_path}", index=False)