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
    # schema = bigQueryDB().getTableColumns("SGX.Tickers")
    # print(schema)

    # ---- Test yFinance Pipeline ---- #
    sgx_data = bigQueryDB().getDataFields("SGX.Tickers").head(20)
    # sgx_data = sgx_data[(sgx_data["ticker"] == "42N") |
    #                     (sgx_data["ticker"] == "5OI")]
    gbq_layer = bigQueryDB()
    yfinance_data_to_upload = yfinanceExtractor(sgx_data).yfinanceQuery()

    # ind_data = yfinance_data_to_upload["stock_industry"]
    # print(ind_data)
    # gen_heat_list = GenerateHeatlistsFromQuery(sgx_data, ind_data)
    # for datafield in yfinance_data_to_upload.keys():
    #     print(datafield)
    #     print(yfinance_data_to_upload[datafield])
    #     # Removing Spaces in Column Names - GBQ Limitation
    #     yfinance_data_to_upload[datafield].columns = yfinance_data_to_upload[datafield].columns.str.replace(
    #         ' ', '_')

    #     # Adding "_" if Column Names start with a number - GBQ Limitation
    #     yfinanace_data_columns = yfinance_data_to_upload[datafield].columns.tolist(
    #     )
    #     yfinance_formatted_columns = {}
    #     for name in yfinanace_data_columns:
    #         if name[0].isdigit():
    #             newName = "_" + name
    #             yfinance_formatted_columns[name] = newName
    #         elif "%" in name:
    #             newName = name.replace('%', 'percentage')
    #             yfinance_formatted_columns[name] = newName
    #         else:
    #             yfinance_formatted_columns[name] = name
    #     print(yfinance_formatted_columns)
    #     yfinance_data_to_upload[datafield].rename(
    #         columns=yfinance_formatted_columns, inplace=True)

    #     datasetTable = "yfinance." + datafield
    #     print(yfinance_data_to_upload[datafield])

    #     yfinance_data_to_upload[datafield] = yfinance_data_to_upload[datafield].convert_dtypes(
    #     )
    #     if gbq_layer.gbqCheckTableExist(datasetTable) and not yfinance_data_to_upload[datafield].empty:
    #         gbq_layer.gbqAppend(
    #             yfinance_data_to_upload[datafield], datasetTable)
    #     elif not yfinance_data_to_upload[datafield].empty:
    #         gbq_layer.gbqCreateNewTable(
    #             yfinance_data_to_upload[datafield], "yfinance", datafield)
    #     else:
    #         print("empty dataframe")
    #
    #
    # ---- Test SGXDataExtractor---- #
    # sgx_layer = SGXDataExtractor()
    # sgx_data = sgx_layer.get_SGX_data()
    # gbq_data = sgx_layer.get_SGXData_from_GBQ()
    # update = sgx_layer.update_ticker_status(sgx_data, gbq_data)
    # print(update)
    # update.to_csv("update.csv", index=False)

    print("--- %s seconds ---" % (time.time() - start_time))
