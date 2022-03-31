import time
from data_extract.SBRExtractor import SBRExtractor
from data_extract.SGXDataExtractor import SGXDataExtractor
from data_extract.TelegramExtractor import TelegramExtractor
from data_load.bigQueryAPI import gbqInjest, gbqQuery
from data_extract.yahooFinNewsExtractor import yahooFinNewsExtractor
from data_pipeline.yahooFinNewsPipeline import yahooFinNewsPipeline
from data_pipeline.FirestorePipeline import FirestorePipeline
from data_pipeline.HeatListPipeline import HeatListPipeline
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
    # formattedData = yahoo_fin_pipeline_layer.tickerNewsFormat(tickerNews)
    # yahoo_fin_pipeline_layer.newsToFirestore()

    # ---- Test FireStore Pipeline ---- #
    # FireStore_layer = FirestorePipeline()
    # FireStore_layer.execute_pipeline(
    #     start_date="15-02-2022", end_date="15-02-2022")

    # ---- Test Heatlist Pipeline ---- #
    HeatListPipeline_layer = HeatListPipeline()
    res = HeatListPipeline_layer.query_pipeline('16-02-2022')
    print(res)
    print(len(res))

    print("--- %s seconds ---" % (time.time() - start_time))
