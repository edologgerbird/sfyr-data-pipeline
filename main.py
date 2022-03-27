import time
from data_extract.SBRExtractor import SBRExtractor
from data_extract.SGXDataExtractor import SGXDataExtractor
from data_extract.TelegramExtractor import TelegramExtractor
from data_load.bigQueryAPI import gbqInjest, gbqQuery


if __name__ == '__main__':
    start_time = time.time()
    sgx_data_extractor_layer = SGXDataExtractor()
    sgx_data_extractor_layer.load_SGX_data_from_source()

    # print("--- %s seconds ---" % (time.time() - start_time))
    # start_time = time.time()

    # sbr_data_extraction_layer = SBRExtractor()
    # sbr_data_extraction_layer.load_SBR_data_from_source(start_date="01-02-2022", end_date="10-03-2022")

    # print("--- %s seconds ---" % (time.time() - start_time))
    # start_time = time.time()

    #tele_data_extractor_layer = TelegramExtractor()

    # Extracts all data
    # tele_data_extractor_layer.extract_telegram_messages()

    # Extracts from start date to end date
    #tele_data_extractor_layer.extract_telegram_messages(start_date="01-02-2022", end_date="10-02-2022")
    # tele_data_extractor_layer.extract_telegram_messages(start_date="01-02-2022", end_date="10-02-2022")

    #print("--- %s seconds ---" % (time.time() - start_time))
