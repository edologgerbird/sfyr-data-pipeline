from numpy import empty
from pendulum import datetime
from data_extract.yfinanceExtractor import yfinanceExtractor
from data_extract.yahooFinNewsExtractor import yahooFinNewsExtractor
from data_extract.yfinanceExtractor import yfinanceExtractor
from data_load.bigQueryAPI import bigQueryDB
from data_extract.TelegramExtractor import TelegramExtractor
from data_extract.SGXDataExtractor import SGXDataExtractor
from data_extract.SBRExtractor import SBRExtractor
from data_processing.generateHeatListFromQuery import GenerateHeatlistsFromQuery
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

    # ---- Test SBR Data Extraction ---- #
    # sbr_data_extraction_layer = SBRExtractor()
    # sbr_data_extraction_layer.load_SBR_data_from_source(start_date="01-02-2022", end_date="10-03-2022")

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
    #                                          "yfinance.ticker_status"
    #                                          ])
    # print(schema)

    # ---- Test yFinance Pipeline ---- #
    # tableSchemaUrl = "utils/bigQuerySchema.json"
    # with open(tableSchemaUrl, 'r') as schemaFile:
    #     tableSchema = json.load(schemaFile)

    # sgx_data = bigQueryDB().getDataFields("SGX.Tickers").sample(15)
    # # sgx_data = sgx_data[(sgx_data["ticker"] == "42N") |
    # #                     (sgx_data["ticker"] == "5OI")]
    # gbq_layer = bigQueryDB()
    # yfinance_data_to_upload = yfinanceExtractor(sgx_data).yfinanceQuery()[
    #     "earnings_and_revenue"]
    # print(yfinance_data_to_upload)

    # gbq_layer.gbqAppend(yfinance_data_to_upload, "yfinance.earnings_and_revenue",
    #                     tableSchema["yfinance.earnings_and_revenue"])

    # ind_data = yfinance_data_to_upload["stock_industry"]
    # print(ind_data)
    # gen_heat_list = GenerateHeatlistsFromQuery(sgx_data, ind_data)

    #
    # ---- Test SGXDataExtractor---- #
    # sgx_layer = SGXDataExtractor()
    # sgx_data = sgx_layer.get_SGX_data()
    # gbq_data = sgx_layer.get_SGXData_from_GBQ()
    # update = sgx_layer.update_ticker_status(sgx_data, gbq_data)
    # print(update)
    # update.to_csv("update.csv", index=False)

    print("--- %s seconds ---" % (time.time() - start_time))
