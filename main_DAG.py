# Importing Airflow Modules
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Importing Pipeline Modules

# General utility Modules
from datetime import datetime, timedelta
improt datetime as dt
import pandas as pd


####################################################
# 1. DEFINE PYTHON FUNCTIONS
####################################################

'''
##############################
# 1. Data Extraction Modules
##############################

def extract_SGX_data():
    >> extracts SGX_data
    >> return DataFrame: SGX_data

def extract_SBR_news_data():
    >> extracts SBR_data
    >> return DataFrame: SBR_data

def extract_Tele_news_data():
    >> extracts Tele_data
    >> return DataFrame: Tele_data

def extract_YahooFin_news_data():
    >> extracts YahooFin_data
    >> return DataFrame: YahooFin_data

def extract_Tiingo_news_data():
    >> extracts Tiingo_data
    >> return DataFrame: Tiingo_data

def extract_yFinance_data():
    >> extract YahooFin_data
    >> return dictionary of DataFrames: YahooFin_data

########################################
# 2. Data Transformation Modules (1)
########################################

def transform_SBR_news_data():
    >> xcom.pull(DataFrame: SBR_news_data)
    >> TickerExtractor(DataFrame: SBR_news)
        >> xcom.pull(DataFrame: SGX Data)
    >> STIMovementExtractor(DataFrame: SBR_news)
    >> FinBERT(DataFrame: SBR_news)
    >> Transform to NoSQL Format
    >> return dictionary: SBR_news_data_transformed

def transform_Tele_news_data():
    >> xcom.pull(DataFrame: Tele_news_data)
    >> TickerExtractor(DataFrame: Tele_news)
        >> xcom.pull(DataFrame: SGX Data)
    >> FinBERT(DataFrame: Tele_news)
    >> Transform to NoSQL Format
    >> return dictionary: Tele_news_data_transformed

def transform_YahooFin_news_data():
    >> xcom.pull(DataFrame: YahooFin_news_data)
    >> TickerExtractor(DataFrame: YahooFin_news)
        >> xcom.pull(DataFrame: SGX Data)
    >> FinBERT(DataFrame: YahooFin_news)
    >> Transform to NoSQL Format
    >> return dictionary: YahooFin_news_data_transformed

def transform_Tiingo_news_data():
    >> xcom.pull(DataFrame: Tiingo_news_data)
    >> TickerExtractor(DataFrame: Tiingo_news)
        >> xcom.pull(DataFrame: SGX Data)
    >> FinBERT(DataFrame: Tiingo_news)
    >> Transform to NoSQL Format
    >> return dictionary: Tiingo_news_data_transformed


###################################
# 3. Data Loading Modules
###################################

def load_SBR_news_data():
    >> xcomm.pull(dictionary: SBR_news_data_transformed)
    >> upload to Firestore Database

def load_Tele_news_data():
    >> xcomm.pull(dictionary: Tele_news_data_transformed)
    >> upload to Firestore Database

def load_YahooFin_news_data():
    >> xcomm.pull(dictionary: YahooFin_news_data_transformed)
    >> upload to Firestore Database

def load_Tiingo_news_data():
    >> xcomm.pull(dictionary: Tiingo_news_data_transformed)
    >> upload to Firestore Database

def load_yFinance_data():
    >> xcomm.pull(dictionary of DataFrames: yFinance_data)
    >> upload to Google BigQuery


########################################
# 3. Data Transformation Modules (2)
########################################

def query_SBR_News_data():
    >> query SBR_News_data from Firestore Database
    >> return dictionary: SBR_News_Query_Results

def query_Tele_News_data():
    >> query Tele_News_data from Firestore Database
    >> return dictionary: Tele_News_Query_Results

def query_YahooFin_News_data():
    >> query YahooFin_News_data from Firestore Database
    >> return dictionary: YahooFin_News_Query_Results

def query_Tiingo_News_data():
    >> query Tiingo_News_data from Firestore Database
    >> return dictionary: Tiingo_News_Query_Results

def generateHeatList():
    >> xcom.pull(
        dictionary: SBR_News_Query_Results, 
        dictionary: Tele_News_Query_Results,
        dictionary: YahooFin_News_Query_Results,
        dictionary: Tiingo_News_Query_Results
        )
    >> Generate Ticker and Industry Heatlists
    >> return DataFrame: Ticker Heatlist, DataFrame: Industry Heatlist

########################################
# 3. Data Load Modules (2)
########################################

def load_heatlists():
    >> xcom.pull(
        DataFrame: Ticker Heatlist, 
        DataFrame: Industry Heatlist
        )
    >> upload to Google Big Query

'''

############################################
#2. DEFINE AIRFLOW DAG (SETTINGS + SCHEDULE)
############################################




##########################################
#3. DEFINE AIRFLOW OPERATORS
##########################################

'''




'''

##########################################
#4. DEFINE OPERATORS HIERARCHY
##########################################