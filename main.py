from numpy import empty
from pendulum import datetime
from data_extract.yfinanceExtractor import yfinanceExtractor
from data_extract.yahooFinNewsExtractor import yahooFinNewsExtractor
from data_extract.yfinanceExtractor import yfinanceExtractor
from data_load.bigQueryAPI import bigQueryDB
from data_extract.TelegramExtractor import TelegramExtractor
from data_extract.SGXDataExtractor import SGXDataExtractor
from data_extract.SBRExtractor import SBRExtractor
from data_transform.generateHeatListFromQuery import GenerateHeatlistsFromQuery
from data_transform.yfinanceTransform import yfinanceTransform
from data_transform.FinBertAPI import FinBERT
from matplotlib import ticker
from datetime import datetime as dt
import time
import pandas as pd
import json

if __name__ == '__main__':
    start_time = time.time()
    # ---- Test SGX Data Extraction ---- #
    # sgx_data_extractor_layer = SGXDataExtractor()
    # sgx_data_extractor_layer.load_SGX_data_from_source()

    # # ---- Test SBR Data Extraction ---- #
    # sbr_data_extraction_layer = SBRExtractor()
    # sbr_data_extraction_layer.load_SBR_data_from_source(
    #     start_date="01-02-2022", end_date="10-03-2022")

    # ---- Test Telegram Data Extraction ---- #
    # tele_data_extractor_layer = TelegramExtractor()

    # Extracts all data
    # tele_data_extractor_layer.extract_telegram_messages()

    # Extracts from start date to end date
    # tele_data_extractor_layer.extract_telegram_messages(start_date="01-02-2022", end_date="10-02-2022")

    # ---- Test YahooFinNews Extraction and Pipeline ---- #
    # tickerNews = yahooFinNewsExtractor().getSGXTickerNews()
    # yahoo_fin_pipeline_layer = yahooFinNewsPipeline()
    # formattedData = yahoo_fin_pipeline_layer.tickerNewsFormat(
    #     tickerNews, dt(2020, 5, 17))
    # yahoo_fin_pipeline_layer.newsToFirestore()

    # ---- Test FireStore Pipeline ---- #
    # FireStore_layer = FirestorePipeline()
    # FireStore_layer.execute_pipeline(
    #     start_date="20-02-2022", end_date="22-02-2022")

    # ---- Test GBQ Pipeline ---- #
    # schema = bigQueryDB().updateTableSchema(["SGX.Tickers",
    #                                          "yfinance.earnings_and_revenue",
    #                                          "yfinance.financial_statements",
    #                                          "yfinance.majorHolders",
    #                                          "yfinance.quarterly_earnings_and_revenue",
    #                                          "yfinance.quarterly_financial_statements",
    #                                          "yfinance.stock_analysis",
    #                                          "yfinance.stock_calendar",
    #                                          "yfinance.stock_ih",
    #                                          "yfinance.stock_industry",
    #                                          "yfinance.stock_info",
    #                                          "yfinance.stock_mfh",
    #                                          "yfinance.stock_recommendation",
    #                                          "yfinance.ticker_status",
    #                                         "yfinance.historical_data"
    #                                          ])
    # print(schema)

    # df = pd.DataFrame(
    #     {
    #         'my_string': ['a', 'b', 'c'],
    #         'my_int64': [1, 2, 3],
    #         'my_float64': [4.0, 5.0, 6.0],
    #         'my_timestamp': [
    #             pd.Timestamp("1998-09-04T16:03:14"),
    #             pd.Timestamp("2010-09-13T12:03:45"),
    #             pd.Timestamp("2015-10-02T16:00:00")
    #         ],
    #     }
    # )

    # ---- Test yFinance Extract ---- #
    # gbq_layer = bigQueryDB()
    # sgx_data = bigQueryDB().getDataFields("SGX.Tickers").head()
    # yfinance_data_to_upload = yfinanceExtractor(sgx_data).yfinanceQuery()

    # ---- Test yFinance Transform ---- #
    # tableSchemaUrl = "utils/bigQuerySchema.json"
    # with open(tableSchemaUrl, 'r') as schemaFile:
    #     tableSchema = json.load(schemaFile)
    # yfinanceTransform_layer = yfinanceTransform(yfinance_data_to_upload)
    # yfinanceTransform_layer.transformData()

    # ---- Test yFinance Upload ---- #
    # gbq_layer.gbqAppend(yfinance_data_to_upload, "yfinance.earnings_and_revenue",
    #                     tableSchema["yfinance.earnings_and_revenue"])

    # ---- Test Heatlist Generation ---- #
    # ind_data = yfinance_data_to_upload["stock_industry"]
    # print(ind_data)
    # gen_heat_list = GenerateHeatlistsFromQuery(sgx_data, ind_data)

    # ---- Test SGXDataExtractor---- #
    # sgx_layer = SGXDataExtractor()
    # sgx_data = sgx_layer.get_SGX_data()
    # gbq_data = sgx_layer.get_SGXData_from_GBQ()
    # update = sgx_layer.update_ticker_status(sgx_data, gbq_data)
    # print(update)
    # update.to_csv("update.csv", index=False)

    # ---- Test FinBert---- #
    # df = pd.read_csv("csv_store/industry_new.csv")
    # fb_layer = FinBERT()
    # fb_layer.FinBert_pipeline(df["industry"])

    print("--- %s seconds ---" % (time.time() - start_time))
