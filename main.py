import pandas as pd
import asyncio
import TelegramExtractor    
import SGXDataExtractor

if __name__ == '__main__':
    sgx_data_extractor_layer = SGXDataExtractor.SGXDataExtractor()
    sgx_data_extractor_layer.get_SGX_data()

    tele_data_extractor_layer = TelegramExtractor.TelegramExtractor()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(tele_data_extractor_layer.extract_telegram_messages())
