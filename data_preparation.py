from sklearn.preprocessing import StandardScaler
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt
from io import StringIO
import pandas as pd
import numpy as np
import logging
import boto3
import time
import os

#create log file if it does not exist
data_preparation_log_file = "C:\\Annie\\Trial\\logs\\data_preparation.log"
logging.root.handlers = []
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO , filename=data_preparation_log_file)

# set up logging
console = logging.StreamHandler()
console.setLevel(logging.ERROR)

# set a format which is simpler for console use
formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(message)s')
console.setFormatter(formatter)
logging.getLogger("").addHandler(console)
    
def prepare_csv_data(output_path="customer_data.csv"):
    
    logging.info("Starting data preparation for csv.")
    # Read data from Amazon S3 bucket
    df = pd.read_csv('data/raw/customer_data.csv')
    
    logging.info("Handling numeric empty data")
    numeric_columns = ['tenure', 'MonthlyCharges', 'TotalCharges'];
    for col in numeric_columns:
        if not pd.api.types.is_numeric_dtype(df[col]):
            df[col] = df[col].str.strip()
            df[col] = df[col].replace('', np.nan)
            df[col] = pd.to_numeric(df[col])
            df[col] = df[col].fillna(df[col].median(skipna=True)) 
    
    logging.info("Converting 'gender', 'SeniorCitizen', 'Partner', 'Dependents', 'PhoneService' as Categorical data.")
    for col in ['gender', 'SeniorCitizen', 'Partner', 'Dependents', 'PhoneService']: # List all categorical columns
        df[col] = df[col].astype('category')
    
    logging.info("Applying drop_first on 'gender', 'SeniorCitizen', 'Partner', 'Dependents', 'PhoneService' to avoid multicollinearity.")
    df = pd.get_dummies(df, columns=['gender', 'Partner', 'InternetService'], drop_first=True) # drop_first avoids multicollinearity
    df['Contract'] = df['Contract'].astype('category').cat.codes 
    
    logging.info("Scaling 'tenure', 'MonthlyCharges', 'TotalCharges'.")
    scaler = StandardScaler()
    df[['tenure', 'MonthlyCharges', 'TotalCharges']] = scaler.fit_transform(df[['tenure', 'MonthlyCharges', 'TotalCharges']])
    
    logging.info("Droping customerID column.")
    
    df = df.drop('customerID', axis=1)

    outdir = 'cleaned'
    if not os.path.exists(outdir):
        os.mkdir(outdir)
        
    logging.info("Saving data to S3.")
    df.to_csv(f"{outdir}/{output_path}", index=False)
    
    
    logging.info("Saving report to S3.")
    generate_report(df)
    
def generate_report(data, pdf_filename = "cleaned/plots.pdf"):   
    
    with PdfPages(pdf_filename) as pdf:
        for column in data.columns:
            logging.info(f"Generating histogram for column {column}.")
            plt.figure(figsize=(8,6))
            plt.hist(data[column], bins=20, alpha=0.7, color='b', edgecolor='black')
            plt.title(f'Histogram of {column}')
            plt.xlabel(column)
            plt.ylabel('Frequency')
            plt.grid(True)
            pdf.savefig()
            plt.close()
            
        columns = data.columns
        for i in range(len(columns)):
            for j in range(i+1, len(columns)):
                logging.info(f"Generating scatter plot for {columns[i]} vs {columns[j]}.")
                
                plt.figure(figsize=(8,6))
                plt.scatter(data[columns[i]], data[columns[j]], alpha=0.7, c='r', edgecolor='k')
                plt.title(f'Scatter plot of {columns[i]} vs {columns[j]}')
                plt.xlabel(columns[i])
                plt.ylabel(columns[j])
                plt.grid(True)
                pdf.savefig()
                plt.close()
        logging.info(f'Saved pdf in {pdf_filename}')


prepare_csv_data("customer_data.csv")