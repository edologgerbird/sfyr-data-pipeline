import config
import pandas as pd
from telethon.sync import TelegramClient

class TelegramExtractor:
    async def extract_telegram_messages(self):
        name = config.teleNumber 
        api_id = config.api_id
        api_hash = config.api_hash 
        chat = 'https://t.me/sgxinvest'

        data = []
        async with TelegramClient(name, api_id, api_hash) as client:
            print('connected to telegram')
            async for message in client.iter_messages(chat):
                data.append([message.date, message.sender_id, message.text])

        print('saved telegram messages to dataframe')
        df = pd.DataFrame(data, columns=['DATE', 'SENDER', 'MESSAGE']) # creates a new dataframe

        df.to_csv('tele_data.csv', encoding='utf-8') # save to a CSV file
        print('exported to csv')
