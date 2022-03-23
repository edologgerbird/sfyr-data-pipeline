import asyncio
from data_extract.TelegramExtractor import TelegramExtractor
from data_extract.SGXDataExtractor import SGXDataExtractor
from data_extract.SBRExtractor import SBRExtractor
import time

if __name__ == '__main__':
    start_time = time.time()
    sgx_data_extractor_layer = SGXDataExtractor()
    sgx_data_extractor_layer.load_SGX_data_from_source()

    print("--- %s seconds ---" % (time.time() - start_time))
    start_time = time.time()

    sbr_data_extraction_layer = SBRExtractor()
    sbr_data_extraction_layer.load_SBR_data_from_source('2022-02-30', '2022-03-10')  #Input start and end date here

    print("--- %s seconds ---" % (time.time() - start_time))
    start_time = time.time()

    tele_data_extractor_layer = TelegramExtractor()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(tele_data_extractor_layer.extract_telegram_messages())

    print("--- %s seconds ---" % (time.time() - start_time))


