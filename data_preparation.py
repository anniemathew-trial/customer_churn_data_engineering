from sklearn.preprocessing import StandardScaler
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt
from pathlib import Path
import seaborn as sns
import pandas as pd
import numpy as np
import logging

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
    try:
        logging.info("Starting data preparation for csv.")
        # Read data from Amazon S3 bucket
        df = pd.read_csv('data/raw/customer_data.csv')
        
        logging.info("Handling 'Tenure', 'Balance', 'EstimatedSalary' empty data")
        numeric_columns = ['Tenure', 'Balance', 'EstimatedSalary'];
        for col in numeric_columns:
            if not pd.api.types.is_numeric_dtype(df[col]):
                df[col] = df[col].str.strip()
                df[col] = df[col].replace('', np.nan)
                df[col] = pd.to_numeric(df[col])
                df[col] = df[col].fillna(df[col].median(skipna=True)) 
        
        logging.info("Handling 'Age' empty data")
        df['Age'] = df['Age'].fillna(df['Tenure'] + 18) 
        
            
        logging.info("Droping 'Surname' as it may lead to profiling, 'RowNumber', 'CustomerId' as it is not required")
        df = df.drop(["RowNumber", "CustomerId", "Surname"], axis = 1)
        
        logging.info("Making 'Geography', 'Gender', 'HasCrCard', 'IsActiveMember' as categorical")
        categorical_columns = ['Geography', 'Gender', 'HasCrCard', 'IsActiveMember']
        df[categorical_columns] = df[categorical_columns].astype('category')

        p = Path('data/cleaned')
        p.mkdir(parents = True, exist_ok = True)
            
        logging.info("Saving data to S3.")
        df.to_csv(f"data/cleaned/{output_path}", index=False)
        
        
        logging.info("Saving report to S3.")
        generate_report(df)
    except Exception as e:
        logging.error(f"Error in preparing data{str(e)}") 
    
def generate_report(data, pdf_filename = "visualization/plots.pdf"):   
    try:
        p = Path('visualization')
        p.mkdir(parents = True, exist_ok = True)
        with PdfPages(pdf_filename) as pdf:
            logging.info("Creating Pie chart")            
            labels = 'Exited', 'Retained'
            sizes = [data.Exited[data['Exited']==1].count(), data.Exited[data['Exited']==0].count()]
            explode = (0, 0.1)
            fig1, ax1 = plt.subplots(figsize=(10, 8))
            ax1.pie(sizes, explode=explode, labels=labels, autopct='%1.1f%%',
                    shadow=True, startangle=90)
            ax1.axis('equal')
            plt.title("Customer Churned vs Retained", size = 20)
            pdf.savefig()
            plt.close()
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
                
            
            logging.info("Generating histogram for relation of Exited with Categorical data.") 
            fig, axarr = plt.subplots(2, 2, figsize=(20, 12))
            sns.countplot(x='Geography', hue = 'Exited',data = data, ax=axarr[0][0])
            sns.countplot(x='Gender', hue = 'Exited',data = data, ax=axarr[0][1])
            sns.countplot(x='HasCrCard', hue = 'Exited',data = data, ax=axarr[1][0])
            sns.countplot(x='IsActiveMember', hue = 'Exited',data = data, ax=axarr[1][1])
            
            logging.info("Generating box plots for relation of Exited with non Categorical data.") 
            fig, axarr = plt.subplots(3, 2, figsize=(20, 12))
            sns.boxplot(y='CreditScore',x = 'Exited', hue = 'Exited',data = data, ax=axarr[0][0])
            sns.boxplot(y='Age',x = 'Exited', hue = 'Exited',data = data , ax=axarr[0][1])
            sns.boxplot(y='Tenure',x = 'Exited', hue = 'Exited',data = data, ax=axarr[1][0])
            sns.boxplot(y='Balance',x = 'Exited', hue = 'Exited',data = data, ax=axarr[1][1])
            sns.boxplot(y='NumOfProducts',x = 'Exited', hue = 'Exited',data = data, ax=axarr[2][0])
            sns.boxplot(y='EstimatedSalary',x = 'Exited', hue = 'Exited',data = data, ax=axarr[2][1])
            
            pdf.savefig()
            plt.close()
            logging.info(f'Saved pdf in {pdf_filename}')
    except Exception as e:
        logging.error(f"Error in creating report{str(e)}")
        


prepare_csv_data("customer_data.csv")