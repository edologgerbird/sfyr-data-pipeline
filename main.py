import asyncio
import TelegramExtractor    
import SGXDataExtractor
import SBRExtractor

if __name__ == '__main__':
    sgx_data_extractor_layer = SGXDataExtractor.SGXDataExtractor()
    sgx_data_extractor_layer.load_SGX_data_from_source()

    sbr_data_extraction_layer = SBRExtractor.SBRExtractor()
    sbr_data_extraction_layer.load_SBR_data_from_source()

    tele_data_extractor_layer = TelegramExtractor.TelegramExtractor()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(tele_data_extractor_layer.extract_telegram_messages())


