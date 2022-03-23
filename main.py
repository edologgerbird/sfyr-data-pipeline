import time
from data_extract.SBRExtractor import SBRExtractor
from data_extract.SGXDataExtractor import SGXDataExtractor
from data_extract.TelegramExtractor import TelegramExtractor

if __name__ == '__main__':
    start_time = time.time()
    sgx_data_extractor_layer = SGXDataExtractor()
    sgx_data_extractor_layer.load_SGX_data_from_source()

    print("--- %s seconds ---" % (time.time() - start_time))
    start_time = time.time()

    sbr_data_extraction_layer = SBRExtractor()
    sbr_data_extraction_layer.load_SBR_data_from_source()

    print("--- %s seconds ---" % (time.time() - start_time))
    start_time = time.time()

    tele_data_extractor_layer = TelegramExtractor.TelegramExtractor()
    # Extracts all data
    # tele_data_extractor_layer.extract_telegram_messages()

    # Extracts from start date to end date
    tele_data_extractor_layer.extract_telegram_messages(start_date="20-02-2022", end_date="01-01-2022")

    # Extracts from latest data until end date
    # tele_data_extractor_layer.extract_telegram_messages(end_date="01-01-2022")
    print("--- %s seconds ---" % (time.time() - start_time))
