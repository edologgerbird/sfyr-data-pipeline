import pandas as pd
from telethon.sync import TelegramClient
from dateutil.parser import parse
from datetime import datetime, timedelta
import asyncio
import json


class TelegramExtractor:
    def __init__(self):
        with open('serviceAccount.json', 'r') as jsonFile:
            self.cred = json.load(jsonFile)
        name = self.cred['telegramConfig']["teleNumber"]
        api_id = self.cred['telegramConfig']["api_id"]
        api_hash = self.cred['telegramConfig']["api_hash"]

        self.tele_data = []
        self.client = TelegramClient(name, api_id, api_hash)

    def extract_telegram_messages(self, start_date=None, end_date=None):
        self.start_date = parse(start_date) if (start_date is not None) else datetime.now()
        self.end_date = parse(end_date) if (end_date is not None) else None

        if (self.end_date is not None and self.start_date is not None and self.end_date > self.start_date):
            raise Exception('End date input must be before start date input')

        print('extracting from:', self.start_date, 'to:', self.end_date)

        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.connect_to_telegram_server())
        loop.run_until_complete(self.populate_tele_data())
        self.tele_data_to_csv()

    async def connect_to_telegram_server(self):
        print('Connecting to Telegram servers...')
        try:
            await self.client.connect()
        except:
            print('Failed to connect: ')

        try:
            if not await self.client.is_user_authorized():
                await self.client.send_code_request(self.name)
                me = await self.client.sign_in(self.name, input('Enter code: '))

                print('connected to telegram')
        except:
            print('Failed to login')

    async def populate_tele_data(self):
        print('Total no. channels to scrape: ', len(self.cred['telegram_channels']))

        for chat in self.cred['telegram_channels']:
            print('Scraping ', chat)
            try:
                async for message in self.client.iter_messages(chat):
                    if (self.end_date is not None and message.date.replace(tzinfo=None) < self.end_date):
                        print('End_date reached, stopping scrape')
                        break

                    if (self.check_date_params(message.date.replace(tzinfo=None))):
                        self.tele_data.append(
                            [chat, message.date, message.sender_id, message.text])
            except Exception as e:
                print('Unknown error while scraping,', e)

        # creates a new dataframe
        self.tele_data = pd.DataFrame(
            self.tele_data, columns=['CHANNEL', 'DATE', 'SENDER', 'MESSAGE'])

        print('Saved telegram messages to dataframe, no. rows = ', len(self.tele_data))

    def check_date_params(self, messageDate):
        return (self.start_date is None and self.end_date is None) or (self.start_date is not None and self.end_date is not None and messageDate < self.start_date and messageDate > self.end_date) or (self.start_date is not None and self.end_date is None and messageDate < self.start_date) or (self.start_date is None and self.end_date is not None and messageDate > self.end_date)

    def tele_data_to_csv(self):
        dateString = self.start_date.strftime("%d-%m-%Y")
        # save to a CSV file
        fileName = f'tele_data_{dateString}.csv'
        self.tele_data.to_csv(fileName, encoding='utf-8')
        print('Exported to csv,', fileName)
