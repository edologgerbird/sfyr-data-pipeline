'''
ETL for Telegram Data, from Source to Firestore
'''
from data_transform.STIMovementExtractor import STIMovementExtractor
from data_extract.TelegramExtractor import TelegramExtractor
from utils.utils import splitter
from datetime import datetime as dt


class telegramNewsPipeline:
    def __init__(self, firestoreDB, tickerExtractor, FinBERT):
        print("Initialising Telegram Data Pipeline")
        self.tele_data_extractor_layer = TelegramExtractor()
        self.ticker_extractor_layer = tickerExtractor
        self.STI_movement_extractor_layer = STIMovementExtractor()
        self.FinBERT_layer = FinBERT
        self.firestoreDB_layer = firestoreDB

        self.tele_data_raw = None

        self.tele_data_processed = None

        self.tele_data_to_upload = None
        print("Firestore Pipeline Initialised")

    # date format: "dd-mm-yyyy"
    def extract_data_from_source(self, start_date, end_date):

        # Extract from Telegram
        self.tele_data_raw = self.tele_data_extractor_layer.extract_telegram_messages(
            start_date=start_date, end_date=end_date)

    def transform_and_process_data(self):

        # Ticker Extraction for Tele Data
        tele_data_with_tickers = self.ticker_extractor_layer.populate_ticker_occurences(
            self.tele_data_raw["message"])

        # NLP for Telegram Data sentiments
        tele_data_with_sentiments = self.FinBERT_layer.FinBert_pipeline(
            self.tele_data_raw["message"]
        )

        # # Combining Dataframes

        tele_data_with_tickers["sentiment"] = [{"sentiment": {
            "positive": x, "negative": y, "neutral": z}} for x, y, z in zip(
            *splitter(tele_data_with_sentiments[["Positive", "Negative", "Neutral"]])
        )]

        # self.SBR_data_processed = SBR_data_with_tickers
        self.tele_data_processed = tele_data_with_tickers
        self.tele_data_processed[["channel", "date", "sender"]
                                 ] = self.tele_data_raw[["channel", "date", "sender"]]

        # Transforming Data to NoSQL format

        # Telegram Data
        self.tele_data_to_upload = [
            {"channel": channel,
             "date": date,
             "sender": sender,
             "message": message,
             "tickers": [ticker for ticker in tickers.keys()],
             "sentiments": list(sentiments.values())[0]}
            for channel, date, sender, message, tickers, sentiments in zip(
                *splitter(self.tele_data_processed[[
                    "channel", "date", "sender", "Text", "Tickers_found", "sentiment"
                ]])
            )
        ]

    def upload_to_firestore(self):
        self.firestoreDB_layer.fsAddListofDocuments(
            "Telegram_data", self.tele_data_to_upload)
        print("Tele Data successfully uploaded.")

    def execute_pipeline(self, start_date, end_date):
        self.extract_data_from_source(start_date, end_date)
        self.transform_and_process_data()
        self.upload_to_firestore()
