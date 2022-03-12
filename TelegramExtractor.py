#import config
import pandas as pd
from telethon.sync import TelegramClient
import json

class TelegramExtractor:
    async def extract_telegram_messages(self):
        with open('serviceAccount.json', 'r') as jsonFile:
            self.cred = json.load(jsonFile)
        name = self.cred['telegramConfig']["teleNumber"] 
        api_id = self.cred['telegramConfig']["api_id"]
        api_hash = self.cred['telegramConfig']["api_hash"] 
        chat = 'https://t.me/sgxinvest'

        data = []
        client = TelegramClient(name, api_id, api_hash)
        print('Connecting to Telegram servers...')
        try:
            await client.connect()
        except:
            print('Failed to connect: ')

        if not await client.is_user_authorized():
            await client.send_code_request(name)
            me = await client.sign_in(name, input('Enter code: '))

        print('connected to telegram')
        async for message in client.iter_messages(chat):
            data.append([message.date, message.sender_id, message.text])

        print('saved telegram messages to dataframe')
        df = pd.DataFrame(data, columns=['DATE', 'SENDER', 'MESSAGE']) # creates a new dataframe

        df.to_csv('tele_data.csv', encoding='utf-8') # save to a CSV file
        print('exported to csv')
