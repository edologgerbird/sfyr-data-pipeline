import pandas as pd
from telethon.sync import TelegramClient
from dateutil.parser import parse
from datetime import datetime, timedelta
import asyncio
import json


class TelegramExtractor:
    def __init__(self):
        with open('utils/serviceAccount.json', 'r') as jsonFile:
            self.cred = json.load(jsonFile)
        self.name = self.cred['telegramConfig']["teleNumber"]
        self.api_id = self.cred['telegramConfig']["api_id"]
        self.api_hash = self.cred['telegramConfig']["api_hash"]

        self.tele_data = []
        self.client = TelegramClient(self.name, self.api_id, self.api_hash)
        print("INFO: TelegramExtractor Initialised")

    def init_tele_session(self):
        """Helper function to initalise Telegram session
        """
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.connect_to_telegram_server())
        print("SUCCESS: Telegram session initiated")
        return

    def extract_telegram_messages(self, start_date=None, end_date=datetime.now()):
        """This function extracts telegram messsages by calling populate_tele_data

        Args:
            start_date (datetime, optional): Earliest Send Date of Message to be extracted. Defaults to None.
            end_date (datetime, optional): Latest Send Date of Message to be extracted. Defaults to datetime.now().

        Raises:
            Exception: Start date input is not before end date input

        Returns:
            list: List of Dataframe where each Dataframe is a single message 
        """

        print("INFO: Extracting Telegram Messages")
        # +- 1 day is to include the date itself in scraping of messages
        # self.start_date = parse(start_date, dayfirst=True) + \
        #     timedelta(days=-1) if (start_date is not None) else None
        # self.end_date = parse(end_date, dayfirst=True) + timedelta(days=1)

        self.start_date = start_date + \
            timedelta(days=-1) if (start_date is not None) else None
        self.end_date = end_date + timedelta(days=1)

        if (self.start_date is not None and self.end_date is not None and self.start_date > self.end_date):
            raise Exception(
                f'ERROR: Start date {self.start_date} input must be before end date {self.end_date} input')

        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.connect_to_telegram_server())
        loop.run_until_complete(self.populate_tele_data())

        return self.tele_data

    async def connect_to_telegram_server(self):
        print('INFO: Connecting to Telegram servers...')
        try:
            await self.client.connect()
        except:
            raise Exception('ERROR: Failed to connect to Telegram Server')

        try:
            if not await self.client.is_user_authorized():
                await self.client.send_code_request(self.name)
                me = await self.client.sign_in(self.name, input('Enter code: '))

                print('SUCCESS: Connected to telegram')
        except:
            raise Exception('ERROR: Failed to login')

    async def populate_tele_data(self):
        print('INFO: Total no. channels to scrape: ',
              len(self.cred['telegram_channels']))

        for chat in self.cred['telegram_channels']:
            print('INFO: Scraping ', chat)
            try:
                async for message in self.client.iter_messages(chat, offset_date=self.end_date):
                    if (self.start_date is not None and message.date.replace(tzinfo=None) < self.start_date + timedelta(days=+1)):
                        print('INFO: start_date reached, stopping scrape')
                        break

                    if (self.check_date_params(message.date.replace(tzinfo=None))):
                        self.tele_data.append(
                            [chat, message.date, message.sender_id, message.text])
            except Exception as e:
                print('ERROR: Unknown error while scraping,', e)

        # creates a new dataframe
        self.tele_data = pd.DataFrame(
            self.tele_data, columns=['channel', 'date', 'sender', 'message'])

        print('SUCCESS: Saved telegram messages to dataframe, no. rows = ',
              len(self.tele_data))

    def check_date_params(self, messageDate):
        """Helper function to Update message date

        Args:
            messageDate (datetime): datetime object of when message is sent

        Returns:
            boolean: True if message is within extract timeframe. False if message is not within extract timeframe. 
        """
        if (self.start_date is not None and self.end_date is not None):
            # takes messages from start_date to end_date
            return messageDate < self.end_date and messageDate > self.start_date
        elif (self.start_date is None and self.end_date is not None):
            # takes messages from start of chat history until end_date
            return messageDate < self.end_date
        else:
            return False

    def tele_data_to_csv(self):
        """Helper function to export telegram data as CSV
        """
        dateString = (self.end_date + timedelta(days=-1)).strftime("%d-%m-%Y")
        # save to a CSV file
        fileName = f'tele_data_{dateString}.csv'
        self.tele_data.to_csv(fileName, encoding='utf-8')
        print('SUCCESS: Exported to csv,', fileName)
