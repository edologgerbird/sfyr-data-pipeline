import time
from data_extract.SBRExtractor import SBRExtractor
from data_extract.SGXDataExtractor import SGXDataExtractor
from data_extract.TelegramExtractor import TelegramExtractor
from data_load.bigQueryAPI import gbqInjest, gbqQuery
from data_extract.yahooFinNews import yahooFinNews
from data_pipeline.yahooFinNewsPipeline import yahooFinNewsPipeline

if __name__ == '__main__':
    start_time = time.time()
    # sgx_data_extractor_layer = SGXDataExtractor()
    # sgx_data_extractor_layer.load_SGX_data_from_source()

    # print("--- %s seconds ---" % (time.time() - start_time))
    # start_time = time.time()

    # sbr_data_extraction_layer = SBRExtractor()
    # sbr_data_extraction_layer.load_SBR_data_from_source()

    # print("--- %s seconds ---" % (time.time() - start_time))
    # start_time = time.time()

    # tele_data_extractor_layer = TelegramExtractor()
    # Extracts all data
    # tele_data_extractor_layer.extract_telegram_messages()

    # Extracts from start date to end date
    # tele_data_extractor_layer.extract_telegram_messages(start_date="01-02-2022", end_date="10-02-2022")

    # Test YahooFinNews Extraction
    tickerNews = yahooFinNews().getSGXTickerNews()
    print(tickerNews)

    # Test YahooFinNews Pipeline
    print(yahooFinNewsPipeline().tickerNewsFormat(tickerNews))
    print("--- %s seconds ---" % (time.time() - start_time))

