import pandas as pd
import asyncio
import TelegramExtractor

if __name__ == '__main__':
    tele_data_extractor_layer = TelegramExtractor.TelegramExtractor()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(tele_data_extractor_layer.extract_telegram_messages())