# Importing Airflow Modules
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Importing High Level Pipeline Modules
# Extract Modules
from data_extract.SGXDataExtractor import SGXDataExtractor
from data_extract.SBRExtractor import SBRExtractor
from data_extract.TelegramExtractor import TelegramExtractor
from data_extract.yahooFinNewsExtractor import yahooFinNewsExtractor


# Transform Modules
from data_transform.STIMovementExtractor import STIMovementExtractor
from data_transform.TickerExtractor import TickerExtractor
from data_transform.SBRDataTransform import SBRDataTransformer
from data_transform.telegramDataTransform import telegramDataTransformer
from data_transform.yahooFinNewsTransform import yahooFinNewsTransform
from data_processing.FinBertAPI import FinBERT
from data_processing.generateHeatListFromQuery import GenerateHeatlistsFromQuery

# Load Modules
from data_load.firestoreAPI import firestoreDB
from data_load.bigQueryAPI import bigQueryDB

# Query Modules
from data_querying.heatlistQuery import HeatListQuery

# will need loading modules for each source

# General utility Modules
from datetime import datetime, timedelta
import datetime as dt
import pandas as pd


####################################################
# 0. DEFINE GLOBAL VARIABLES
####################################################

global_start_date = datetime.now()
global_end_date = datetime.now()

firestoreDB_layer = firestoreDB()
bigQueryDB_layer = bigQueryDB()

FinBERT_layer = FinBERT()

HeatListDataQuery_layer = HeatListQuery(firestoreDB_layer)


####################################################
# 1. DEFINE PYTHON FUNCTIONS
####################################################

##############################
# 1A. Data Extraction Modules
##############################

def extract_SGX_data(**kwargs):
    # >> extracts SGX_data
    # >> return DataFrame: SGX_data
    SGXDataExtractor_layer = SGXDataExtractor()
    SGXDataExtractor_layer.load_SGX_data_from_source()
    sgx_data = SGXDataExtractor_layer.get_SGX_data()
    return sgx_data


def extract_SBR_data(**kwargs):
    # >> extracts SBR_data
    # >> return DataFrame: SBR_data
    SBRExtractor_layer = SBRExtractor()
    sbr_raw_data = SBRExtractor_layer.load_SBR_data_from_source(
        start_date=global_start_date, end_date=global_end_date)
    return sbr_raw_data


def extract_tele_data(**kwargs):
    # >> extracts tele_data
    # >> return DataFrame: tele_data
    TelegramExtractor_layer = TelegramExtractor()
    tele_data_raw = TelegramExtractor_layer.extract_telegram_messages(
        start_date=global_start_date, end_date=global_end_date)
    return tele_data_raw


def extract_YahooFin_data(**kwargs):
    # >> extracts YahooFin_data
    # >> return DataFrame: YahooFin_data
    yahooFinNewsExtractor_layer = yahooFinNewsExtractor()
    # pending time periood
    yahooFinNews_data_raw = yahooFinNewsExtractor_layer.getSGXTickerNews()
    return yahooFinNews_data_raw


def extract_yFinance_data(**kwargs):
    # >> extract YahooFin_data
    # >> return dictionary of DataFrames: YahooFin_data
    return

########################################
# 1B. Data Transformation Modules (1)
########################################


def query_SGX_data(**kwargs):
    # >> query recent SGX data from GBQ
    # >> return DataFrame: SGX_data
    return


def transform_SGX_data(**kwargs):
    # >> xcom.pull(DataFrame: SGX_data)
    # >> compares SGX_data extracted from SGX source and SGX_data queried from GBQ. New column to indicate "Active" or "Delisted"
    # >> Initialise TickerExtractor with SGX_data_new
    # >> returns TickerExtractor: TickerExtractorLayer
    return


def transform_SBR_data(**kwargs):

    ti = kwargs['ti']

    # >> xcom.pull(DataFrame: SBR_news_data)
    SBR_data_raw = ti.xcom_pull(task_ids="extract_SBR_data_task")

    # >> TickerExtractor(DataFrame: SBR_news)
    #     >> xcom.pull(tickerExtractor: ticker_extractor_layer)
    ticker_extractor_layer = ti.xcom_pull(task_ids="transform_SGX_data_task")

    SBR_data_with_tickers = ticker_extractor_layer.populate_ticker_occurences(
        SBR_data_raw["Title"] + " " + SBR_data_raw["Text"])

    # >> STIMovementExtractor(DataFrame: SBR_news)
    STI_movement_extractor_layer = STIMovementExtractor()

    SBR_data_with_tickers[['STI_direction', 'STI_movement']] = STI_movement_extractor_layer.populate_sti_movement(
        SBR_data_raw['Text'])[['Direction of STI Movement', 'Percentage of STI Movement']]

    # >> FinBERT(DataFrame: SBR_news)
    SBR_data_with_sentiments = FinBERT_layer.FinBert_pipeline(
        SBR_data_raw["Title"] + " " + SBR_data_raw["Text"])

    # >> Transform to NoSQL Format
    SBRDataTransformer_layer = SBRDataTransformer()
    SBR_data_transformed = SBRDataTransformer_layer.transformSBRData(
        SBR_data_raw, SBR_data_with_tickers, SBR_data_with_sentiments)

    # >> return dictionary: SBR_news_data_transformed
    return SBR_data_transformed


def transform_tele_data(**kwargs):

    ti = kwargs['ti']

    # >> xcom.pull(DataFrame: tele_news_data)
    tele_data_raw = ti.xcom_pull(task_ids="extract_tele_data_task")

    # >> TickerExtractor(DataFrame: tele_news)
    #     >> xcom.pull(DataFrame: SGX Data_new)
    ticker_extractor_layer = ti.xcom_pull(task_ids="transform_SGX_data_task")
    tele_data_with_tickers = ticker_extractor_layer.populate_ticker_occurences(
        tele_data_raw["message"])

    # >> FinBERT(DataFrame: tele_news)
    tele_data_with_sentiments = FinBERT_layer.FinBert_pipeline(
        tele_data_raw["message"])

    # >> Transform to NoSQL Format
    # >> return dictionary: tele_news_data_transformed
    telegramDataTransformer_layer = telegramDataTransformer()
    tele_data_transformed = telegramDataTransformer_layer.transformTelegramData(
        tele_data_raw, tele_data_with_tickers, tele_data_with_sentiments)

    return tele_data_transformed


def transform_YahooFin_data(**kwargs):
    ti = kwargs['ti']
    # >> xcom.pull(DataFrame: YahooFin_news_data)
    yahoo_fin_data = ti.xcom_pull(task_ids="extract_YahooFin_data_task")
    yahooFinNewsTransform_layer = yahooFinNewsTransform()
    news_formatted = yahooFinNewsTransform_layer.tickerNewsFormat(yahoo_fin_data, start_date=global_start_date, end_date=global_end_date)
    yahoo_fin_data_sentiments = FinBERT_layer.FinBert_pipeline(news_formatted["message"])
    yahoo_fin_data_transformed = yahooFinNewsTransform_layer(yahoo_fin_data_sentiments)
    return yahoo_fin_data_transformed


def transform_yFinance_data(**kwargs):
    # >> xcom.pull(DataFrame: yFinance_data)
    # >> Transform
    # >> return list of dictionary: yFinance_data_transformed
    return



###################################
# 1C. Data Loading Modules (1)
###################################

def load_SGX_data(**kwargs):
    # >> xcom.pull(DataFrame: SGX_data_new)
    # >> upload to GBQ
    return


def load_SBR_data(**kwargs):
    ti = kwargs['ti']
    # >> xcomm.pull(dictionary: SBR_news_data_transformed)
    SBR_data_to_upload = ti.xcom_pull(task_ids='transform_SBR_data_task')
    # >> upload to Firestore Database
    firestoreDB_layer.fsAddListofDocuments("SBR_data", SBR_data_to_upload)

def load_tele_data(**kwargs):
    ti = kwargs['ti']
    # >> xcomm.pull(dictionary: tele_news_data_transformed)
    tele_data_to_upload = ti.xcom_pull(task_ids='transform_tele_data_task')
    # >> upload to Firestore Database
    firestoreDB_layer.fsAddListofDocuments("Telegram_data", tele_data_to_upload)


def load_YahooFin_news_data(**kwargs):
    ti = kwargs['ti']
    # >> xcomm.pull(dictionary: YahooFin_news_data_transformed)
    yahoo_fin_data_to_upload = ti.xcom_pull(task_ids='transform_YahooFin_data_task')
    # >> upload to Firestore Database
    firestoreDB_layer.fsAddListofDocuments("YahooFin_data", yahoo_fin_data_to_upload)


def load_yFinance_data(**kwargs):
    # >> xcomm.pull(dictionary of DataFrames: yFinance_data)
    # >> upload to Google BigQuery
    return


########################################
# 1D. Data Transformation Modules (2)
########################################


def query_SBR_data(**kwargs):
    # >> query SBR_news_data from Firestore Database
    SBR_query_for_heatlist = HeatListDataQuery_layer.query_pipeline(
        "SBR_data", global_start_date)
    # >> return dictionary: SBR_news_Query_Results
    return SBR_query_for_heatlist


def query_tele_data(**kwargs):
    # >> query tele_news_data from Firestore Database
    tele_query_for_heatlist = HeatListDataQuery_layer.query_pipeline(
        "Telegram_data", global_start_date)
    # >> return dictionary: tele_news_Query_Results
    return tele_query_for_heatlist


def query_YahooFin_news_data(**kwargs):
    # >> query YahooFin_news_data from Firestore Database
    yahoo_fin_query_for_heatlist = HeatListDataQuery_layer.query_pipeline(
        "YahooFin_data", global_start_date)
    # >> return dictionary: YahooFin_news_Query_Results
    return yahoo_fin_query_for_heatlist


def generateHeatlists(**kwargs):
    ti = kwargs['ti']
    # >> xcom.pull(
    #     dictionary: SBR_news_Query_Results,
    #     dictionary: tele_news_Query_Results,
    #     dictionary: YahooFin_news_Query_Results
    # )
    SBR_day_data_for_heatlist = ti.xcom_pull(
        task_ids='transform_SBR_data_task')
    tele_day_data_for_heatlist = ti.xcom_pull(
        task_ids='transform_tele_data_task')

    SBR_query_for_heatlist = ti.xcom_pull(task_ids='query_SBR_data_task')
    tele_query_for_heatlist = ti.xcom_pull(task_ids='query_tele_data_task')

    query_documents_container = SBR_query_for_heatlist + \
        SBR_day_data_for_heatlist + tele_day_data_for_heatlist + tele_query_for_heatlist
    # >> Generate Ticker and Industry Heatlists
    GenerateHeatlistsFromQuery_layer = GenerateHeatlistsFromQuery()
    ticker_heatlist, industry_heatlist = GenerateHeatlistsFromQuery_layer.HeatlistPipeline_execute(
        query_documents_container)
    # >> return DataFrame: Ticker Heatlist, DataFrame: Industry Heatlist
    return (ticker_heatlist, industry_heatlist)


########################################
# 1E. Data Load Modules (2)
########################################
def load_heatlists(**kwargs):
    # >> xcom.pull(
    #     DataFrame: Ticker Heatlist,
    #     DataFrame: Industry Heatlist
    #     )
    # >> upload to Google Big Query
    return

############################################
# 2. DEFINE AIRFLOW DAG (SETTINGS + SCHEDULE)
############################################


default_args = {
    'owner': 'is3107_g7',
    'depends_on_past': False,
    'email': ['is3107_g7@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1
}

dag = DAG('_ETL_for_SGX_Stocks_Data',
          default_args=default_args,
          description='Collect Stock Prices For Analysis',
          catchup=False,
          start_date=global_start_date,  # may need to timedelta(days = 1)
          schedule_interval='0 0 * * *'  # runs daily
          )


##########################################
# 3. DEFINE AIRFLOW OPERATORS
##########################################


##############################
# 3A. Data Extraction Tasks
##############################

extract_SGX_data_task = PythonOperator(task_id='extract_SGX_data_task',
                                       python_callable=extract_SGX_data,
                                       provide_context=True, dag=dag)


extract_SBR_data_task = PythonOperator(task_id='extract_SBR_data_task',
                                       python_callable=extract_SBR_data,
                                       provide_context=True, dag=dag)

extract_tele_data_task = PythonOperator(task_id='extract_tele_data_task',
                                        python_callable=extract_tele_data,
                                        provide_context=True, dag=dag)

extract_YahooFin_data_task = PythonOperator(task_id='extract_YahooFin_data_task',
                                            python_callable=extract_YahooFin_data,
                                            provide_context=True, dag=dag)


extract_yFinance_data_task = PythonOperator(task_id='extract_yFinance_data_task',
                                            python_callable=extract_yFinance_data,
                                            provide_context=True, dag=dag)


####################################
# 3B. Data Transformation Tasks (1)
####################################

query_SGX_data_task = PythonOperator(task_id='query_SGX_data_task',
                                     python_callable=query_SGX_data,
                                     provide_context=True, dag=dag)

transform_SGX_data_task = PythonOperator(task_id='transform_SGX_data_task',
                                         python_callable=transform_SGX_data,
                                         provide_context=True, dag=dag)

transform_SBR_data_task = PythonOperator(task_id='transform_SBR_data_task',
                                         python_callable=transform_SBR_data,
                                         provide_context=True, dag=dag)

transform_tele_data_task = PythonOperator(task_id='transform_tele_data_task',
                                          python_callable=transform_tele_data,
                                          provide_context=True, dag=dag)

transform_YahooFin_data_task = PythonOperator(task_id='transform_YahooFin_data_task',
                                              python_callable=transform_YahooFin_data,
                                              provide_context=True, dag=dag)

transform_yFinance_data_task = PythonOperator(task_id='transform_yFinance_data_task',
                                              python_callable=transform_yFinance_data,
                                              provide_context=True, dag=dag)


# Does yFinance data need transforming?

##############################
# 3C. Data Loading Tasks (1)
##############################

load_SGX_data_task = PythonOperator(task_id='load_SGX_data_task',
                                    python_callable=load_SGX_data,
                                    provide_context=True, dag=dag)

load_SBR_data_task = PythonOperator(task_id='load_SBR_data_task',
                                    python_callable=load_SBR_data,
                                    provide_context=True, dag=dag)

load_tele_data_task = PythonOperator(task_id='load_tele_data_task',
                                     python_callable=load_tele_data,
                                     provide_context=True, dag=dag)

load_YahooFin_data_task = PythonOperator(task_id='load_YahooFin_data_task',
                                         python_callable=load_YahooFin_news_data,
                                         provide_context=True, dag=dag)

load_yFinance_data_task = PythonOperator(task_id='load_yFinance_data_task',
                                         python_callable=load_yFinance_data,
                                         provide_context=True, dag=dag)

####################################
# 3D. Data Transformation Tasks (2)
####################################

query_SBR_data_task = PythonOperator(task_id='query_SBR_data_task',
                                     python_callable=query_SBR_data,
                                     provide_context=True, dag=dag)

query_tele_data_task = PythonOperator(task_id='query_tele_data_task',
                                      python_callable=query_tele_data,
                                      provide_context=True, dag=dag)

query_YahooFin_data_task = PythonOperator(task_id='query_YahooFin_data_task',
                                          python_callable=query_YahooFin_news_data,
                                          provide_context=True, dag=dag)

generate_heatlists_task = PythonOperator(task_id='generate_heatlists_task',
                                         python_callable=generateHeatlists,
                                         provide_context=True, dag=dag)

##############################
# 3E. Data Loading Tasks (2)
##############################

load_heatlists_task = PythonOperator(task_id='load_heatlists_task',
                                     python_callable=load_heatlists,
                                     provide_context=True, dag=dag)


##########################################
# 4. DEFINE OPERATORS HIERARCHY
##########################################

[[extract_SGX_data_task >> query_SGX_data_task >> transform_SGX_data_task], extract_tele_data_task, extract_SBR_data_task] 

extract_tele_data_task >> transform_tele_data_task

extract_SBR_data_task >> transform_SBR_data_task

transform_SGX_data_task >> [transform_tele_data_task, extract_yFinance_data_task, load_SGX_data_task, extract_YahooFin_data_task,transform_SBR_data_task]

transform_tele_data_task >> query_tele_data_task >> generate_heatlists_task

transform_tele_data_task >> load_tele_data_task

transform_SBR_data_task >> load_SBR_data_task

extract_yFinance_data_task >> transform_yFinance_data_task >> load_yFinance_data_task

extract_YahooFin_data_task >> transform_YahooFin_data_task >> [load_YahooFin_data_task, query_YahooFin_data_task]

transform_SBR_data_task >> query_SBR_data_task >> generate_heatlists_task

query_YahooFin_data_task >> generate_heatlists_task

generate_heatlists_task >> load_heatlists_task


# >> [[transform_tele_data_task >> [load_tele_data_task, query_tele_data_task]], [transform_SBR_data_task >> [load_SBR_data_task, query_SBR_data_task]], [extract_yFinance_data_task >> transform_yFinance_data_task >> load_yFinance_data_task], load_SGX_data_task, [extract_YahooFin_data_task >> transform_YahooFin_data_task >> [query_YahooFin_data_task, load_YahooFin_data_task]]] >> generate_heatlists_task >> load_heatlists_task

'''
a = [extract_SGX_data_task >> query_SGX_data_task >> transform_SGX_data_task]

b = [extract_tele_data_task >> transform_tele_data_task]

c = [extract_SBR_data_task >> transform_SBR_data_task]

d = [extract_yFinance_data_task,
     transform_yFinance_data_task, load_yFinance_data_task]

e = [extract_YahooFin_data_task >> transform_YahooFin_data_task >>
     [load_YahooFin_data_task, query_YahooFin_data_task]]

a >> [b, c]
'''

'''
extract_SGX_data_task >> query_SGX_data_task>> transform_SGX_data_task >> load_SGX_data_task >> extract_SBR_data_task >> extract_tele_data_task >> \
extract_YahooFin_data_task >> extract_yFinance_data_task >> transform_yFinance_data_task >> \
transform_SBR_data_task >> transform_tele_data_task >> transform_YahooFin_data_task >> \
load_SBR_data_task >> load_tele_data_task >> load_YahooFin_data_task >> \
load_yFinance_data_task >> query_SBR_data_task >> query_tele_data_task >> \
query_YahooFin_data_task >> generate_heatlists_task >> load_heatlists_task

'''
