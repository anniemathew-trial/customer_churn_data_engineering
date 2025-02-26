from pathlib import Path
import pandas as pd
import logging
import pyodbc

#create log file if it does not exist
ingestion_log_file = "C:\\Annie\\Trial\\logs\\data_ingestion.log"
logging.root.handlers = []
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO , filename=ingestion_log_file)

# set up logging
console = logging.StreamHandler()
console.setLevel(logging.ERROR)

# set a format which is simpler for console use
formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(message)s')
console.setFormatter(formatter)
logging.getLogger("").addHandler(console)

def ingest_csv(filename):
    try:
        logging.info(f"Reading data from CSV file {filename}")
        data = pd.read_csv(filename)
        p = Path('data/raw')
        p.mkdir(parents = True, exist_ok = True)
        data.to_csv(f"data/raw/{filename}", index=False)
        logging.info(f"Data from CSV {filename} ingested successfully!")
        return data
    except Exception as e:
        logging.error(f"Error ingesting CSV: {str(e)}")
        
def ingest_database():
    try:
        server = "127.0.0.1,1433"
        database = "dmml_assignment"
        username = "sa"
        password = "1StrongPwd!!"

        logging.info("Connecting to Database")
        connection = pyodbc.connect(f'DRIVER={{SQL SERVER}};SERVER={server};DATABASE={database};UID={username};PWD={password}')
        logging.info("Connecting to Database Successfull")
        _ = connection.cursor()
        
        query = "SELECT * FROM [dmml_assignment].[dbo].[customers]"
        logging.info("Reading data from database.")
        data = pd.read_sql(query, connection)
        
        connection.close()
        p = Path('data/raw')
        p.mkdir(parents = True, exist_ok = True)
        data.to_csv("data/raw/database_data.csv", index=False)
        logging.info("Data from Database ingested successfully!")
        return data
    except Exception as e:
        logging.error(f"Error ingesting database: {str(e)}")
        
filename = 'customer_data.csv';
csv_data = ingest_csv(filename)
database_data = ingest_database()