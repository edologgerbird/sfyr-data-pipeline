# Importing Airflow Modules
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Importing High Level Pipeline Modules
# Extract Modules
from data_extract.SBRExtractor import SBRExtractor
from data_extract.TelegramExtractor import TelegramExtractor
from data_extract.yahooFinNewsExtractor import yahooFinNewsExtractor

# Transform Modules
from data_transform.STIMovementExtractor import STIMovementExtractor
from data_transform.TickerExtractor import TickerExtractor
from data_processing.FinBertAPI import FinBERT
from data_processing.HeatListGenerator import HeatListGenerator

# Load Modules
from data_load.firestoreAPI import firestoreDB
from data_load.bigQueryAPI import bigQueryDB
# will need loading modules for each source

# General utility Modules
from datetime import datetime, timedelta
import datetime as dt
import pandas as pd


####################################################
# 0. DEFINE GLOBAL VARIABLES
####################################################

'''
start_date = datetime: start_date
end_date = datetime: end_date

firestoreDBLayer = firestoreDB: firestoreDBLayer
bigQueryDBLayer = bigQueryDB: bigQueryDBLayer

tickerExtractionLayer = TickerExtractor: tickerExtractionLayer
FinBERTLayer = FinBERT: FinBERTLayer
'''


####################################################
# 1. DEFINE PYTHON FUNCTIONS
####################################################

'''

##############################
# 1A. Data Extraction Modules
##############################

def extract_SGX_data(**kwargs):
    >> extracts SGX_data
    >> return DataFrame: SGX_data

def extract_SBR_news_data(**kwargs):
    >> extracts SBR_data
    >> return DataFrame: SBR_data

def extract_Tele_news_data(**kwargs):
    >> extracts Tele_data
    >> return DataFrame: Tele_data

def extract_YahooFin_news_data(**kwargs):
    >> extracts YahooFin_data
    >> return DataFrame: YahooFin_data

def extract_yFinance_data(**kwargs):
    >> extract YahooFin_data
    >> return dictionary of DataFrames: YahooFin_data

########################################
# 1B. Data Transformation Modules (1)
########################################

def transform_SBR_news_data(**kwargs):
    >> xcom.pull(DataFrame: SBR_news_data)
    >> TickerExtractor(DataFrame: SBR_news)
        >> xcom.pull(DataFrame: SGX Data)
    >> STIMovementExtractor(DataFrame: SBR_news)
    >> FinBERT(DataFrame: SBR_news)
    >> Transform to NoSQL Format
    >> return dictionary: SBR_news_data_transformed

def transform_Tele_news_data(**kwargs):
    >> xcom.pull(DataFrame: Tele_news_data)
    >> TickerExtractor(DataFrame: Tele_news)
        >> xcom.pull(DataFrame: SGX Data)
    >> FinBERT(DataFrame: Tele_news)
    >> Transform to NoSQL Format
    >> return dictionary: Tele_news_data_transformed

def transform_YahooFin_news_data(**kwargs):
    >> xcom.pull(DataFrame: YahooFin_news_data)
    >> TickerExtractor(DataFrame: YahooFin_news)
        >> xcom.pull(DataFrame: SGX Data)
    >> FinBERT(DataFrame: YahooFin_news)
    >> Transform to NoSQL Format
    >> return dictionary: YahooFin_news_data_transformed

## Does yFinance data need transformating?


###################################
# 1C. Data Loading Modules (1)
###################################

def load_SBR_news_data(**kwargs):
    >> xcomm.pull(dictionary: SBR_news_data_transformed)
    >> upload to Firestore Database

def load_Tele_news_data(**kwargs):
    >> xcomm.pull(dictionary: Tele_news_data_transformed)
    >> upload to Firestore Database

def load_YahooFin_news_data(**kwargs):
    >> xcomm.pull(dictionary: YahooFin_news_data_transformed)
    >> upload to Firestore Database

def load_yFinance_data(**kwargs):
    >> xcomm.pull(dictionary of DataFrames: yFinance_data)
    >> upload to Google BigQuery


########################################
# 1D. Data Transformation Modules (2)
########################################

def query_SBR_news_data(**kwargs):
    >> query SBR_news_data from Firestore Database
    >> return dictionary: SBR_news_Query_Results

def query_Tele_news_data(**kwargs):
    >> query Tele_news_data from Firestore Database
    >> return dictionary: Tele_news_Query_Results

def query_YahooFin_news_data(**kwargs):
    >> query YahooFin_news_data from Firestore Database
    >> return dictionary: YahooFin_news_Query_Results

def generateHeatlists(**kwargs):
    >> xcom.pull(
        dictionary: SBR_news_Query_Results, 
        dictionary: Tele_news_Query_Results,
        dictionary: YahooFin_news_Query_Results
        )
    >> Generate Ticker and Industry Heatlists
    >> return DataFrame: Ticker Heatlist, DataFrame: Industry Heatlist

########################################
# 1E. Data Load Modules (2)
########################################

def load_heatlists(**kwargs):
    >> xcom.pull(
        DataFrame: Ticker Heatlist, 
        DataFrame: Industry Heatlist
        )
    >> upload to Google Big Query

'''

############################################
#2. DEFINE AIRFLOW DAG (SETTINGS + SCHEDULE)
############################################


default_args = {
     'owner': 'is3107_g7',
     'depends_on_past': False,
     'email': ['is3107_g7@gmail.com'],
     'email_on_failure': True,
     'email_on_retry': True,
     'retries': 1
    }

dag = DAG( 'ETL_for_SGX_Stocks_Data',
            default_args = default_args,
            description = 'Collect Stock Prices For Analysis',
            catchup = False, 
            start_date = start_date, # may need to timedelta(days = 1)
            schedule_interval = '0 0 * * *'  # runs daily
          )  




##########################################
#3. DEFINE AIRFLOW OPERATORS
##########################################



##############################
# 3A. Data Extraction Tasks
##############################
'''
extract_SGX_data_task = PythonOperator(task_id = 'extract_SGX_data_task', 
                                        python_callable = extract_SGX_data, 
                                        provide_context=True, dag = dag)


extract_SBR_data_task = PythonOperator(task_id = 'extract_SBR_data_task', 
                                        python_callable = extract_SBR_news_data, 
                                        provide_context=True, dag = dag)


extract_Tele_data_task = PythonOperator(task_id = 'extract_Tele_data_task', 
                                        python_callable = extract_Tele_news_data, 
                                        provide_context=True, dag = dag)

extract_YahooFin_data_task = PythonOperator(task_id = 'extract_YahooFin_data_task', 
                                        python_callable = extract_YahooFin_news_data, 
                                        provide_context=True, dag = dag)

extract_yFinance_data_task = PythonOperator(task_id = 'extract_yFinance_data_task', 
                                        python_callable = extract_yFinance_news_data, 
                                        provide_context=True, dag = dag)

####################################
# 3B. Data Transformation Tasks (1)
####################################

transform_SBR_data_task = PythonOperator(task_id = 'transform_SBR_data_task', 
                                        python_callable = transform_SBR_news_data, 
                                        provide_context=True, dag = dag)

transform_Tele_data_task = PythonOperator(task_id = 'transform_Tele_data_task', 
                                        python_callable = transform_Tele_news_data, 
                                        provide_context=True, dag = dag)

transform_YahooFin_data_task = PythonOperator(task_id = 'transform_YahooFin_data_task', 
                                        python_callable = transform_YahooFin_news_data, 
                                        provide_context=True, dag = dag)

## Does yFinance data need transforming?

##############################
# 3C. Data Loading Tasks (1)
##############################

load_SBR_data_task = PythonOperator(task_id = 'load_SBR_data_task', 
                                        python_callable = load_SBR_news_data, 
                                        provide_context=True, dag = dag)

load_Tele_data_task = PythonOperator(task_id = 'load_Tele_data_task', 
                                        python_callable = load_Tele_news_data, 
                                        provide_context=True, dag = dag)

load_YahooFin_data_task = PythonOperator(task_id = 'load_YahooFin_data_task', 
                                        python_callable = load_YahooFin_news_data, 
                                        provide_context=True, dag = dag)

load_yFinance_data_task = PythonOperator(task_id = 'load_yFinance_data_task', 
                                        python_callable = load_yFinance_news_data, 
                                        provide_context=True, dag = dag)

####################################
# 3D. Data Transformation Tasks (2)
####################################

query_SBR_data_task = PythonOperator(task_id = 'query_SBR_data_task', 
                                        python_callable = query_SBR_news_data, 
                                        provide_context=True, dag = dag)

query_Tele_data_task = PythonOperator(task_id = 'query_Tele_data_task', 
                                        python_callable = query_Tele_news_data, 
                                        provide_context=True, dag = dag)

query_YahooFin_data_task = PythonOperator(task_id = 'query_YahooFin_data_task', 
                                        python_callable = query_YahooFin_news_data, 
                                        provide_context=True, dag = dag)

generate_heatlists_task = PythonOperator(task_id = 'generate_heatlists_task', 
                                        python_callable = generateHeatlists, 
                                        provide_context=True, dag = dag)

##############################
# 3E. Data Loading Tasks (2)
##############################

load_heatlists_task = PythonOperator(task_id = 'load_SBR_data_task', 
                                        python_callable = load_heatlists, 
                                        provide_context=True, dag = dag)

'''

##########################################
#4. DEFINE OPERATORS HIERARCHY
##########################################

'''
extract_SGX_data_task >> extract_SBR_data_task >> extract_Tele_data_task >> \
extract_YahooFin_data_task >> extract_yFinance_data_task >> \
transform_SBR_data_task >> transform_Tele_data_task >> transform_YahooFin_data_task >> \
load_SBR_data_task >> load_Tele_data_task >> load_YahooFin_data_task >> \
load_yFinance_data_task >> query_SBR_data_task >> query_Tele_data_task >> \
query_YahooFin_data_task >> generate_heatlists_task >> load_heatlists_task


'''