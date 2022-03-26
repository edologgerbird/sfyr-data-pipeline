'''
Firestore Data Pipeline
1. Scrapes data from SBR and Telegram
2. Extracts tickers from text data
3. Applies FinBERT to extract sentiments
4. Upload to Firestore
'''
# Data Extraction
from data_extract.SBRExtractor import SBRExtractor
from data_extract.TelegramExtractor import TelegramExtractor
# Data Transforming
from data_transform.TickerExtractor import TickerExtractor
from data_processing.FinBertAPI import FinBERT
# Data Loading
from data_load.firestoreAPI import firestoreDB

import json
import pandas as pd
import numpy as np


class FirestorePipeline:
    def __init__(self):
        print("Initialising Firestore Pipeline...")
        self.tele_data_extractor_layer = TelegramExtractor()
        self.sbre_data_extractor_layer = SBRExtractor()
        self.ticker_extractor_layer = TickerExtractor()
        self.FinBERT_layer = FinBERT()
        self.firestoreDB_layer = firestoreDB()

        self.tele_data_raw = None
        self.SBR_data_raw = None

        self.tele_data_processed = None
        self.SBR_data_processed = None

        self.tele_data_to_upload = None
        self.SBR_data_to_upload = None
        print("Firestore Pipeline Initialised")

    def extract_data_from_source(self, start_date, end_date):
        # Extract from SBR
        self.SBR_data_raw = None
        # Extract from Telegram
        self.tele_data_raw = self.tele_data_extractor_layer.extract_telegram_messages(
            start_date=start_date, end_date=end_date)

    def transform_and_process_data(self):
        # Ticker Extraction for SBR Data
        SBR_data_with_tickers = self.ticker_extractor_layer.populate_ticker_occurences(
            self.SBR_data_raw["Title"] + " " + self.SBR_data_raw["Text"])
        # Ticker Extraction for Tele data
        tele_data_with_tickers = self.ticker_extractor_layer.populate_ticker_occurences(
            self.tele_data_raw["message"])

        # NLP for SBR Data sentiments
        SBR_data_with_sentiments = self.FinBERT_layer.FinBert_pipeline(
            self.SBR_data_raw["Title"] + " " + self.SBR_data_raw["Text"])

        # NLP for Telegram Data sentiments
        tele_data_with_sentiments = self.FinBERT_layer.FinBert_pipeline(
            self.tele_data_raw["message"]
        )

        # Combining Dataframes
        SBR_data_with_tickers["sentiment"] = {"sentiment": {
            "postive": x, "negative": y, "neutral": z} for x, y, z in zip(
            *self.splitter(SBR_data_with_sentiments["Positive", "Negative", "Neutral"])
        )}

        tele_data_with_tickers["sentiment"] = {"sentiment": {
            "postive": x, "negative": y, "neutral": z} for x, y, z in zip(
            *self.splitter(tele_data_with_sentiments["Positive", "Negative", "Neutral"])
        )}

        self.SBR_data_processed = SBR_data_with_tickers
        self.SBR_data_processed[["Date", "Title", "Text", "Link"]
                                ] = self.SBR_data_raw[["Date", "Title", "Text", "Link"]]

        self.tele_data_processed = tele_data_with_tickers
        self.tele_data_processed[["channel", "date", "sender"]
                                 ] = self.tele_data_raw[["channel", "date", "sender"]]

        # Transforming Data to NoSQL format

        # SBR Data
        self.SBR_data_to_upload = [
            {"text_headline": headline,
             "text_body": body,
             "link": link,
             "date": date,
             "tickers": json.loads(tickers),
             "sentiments": json.loads(sentiments)}
            for headline, body, link, date, tickers, sentiments in zip(
                *self.splitter(self.SBR_data_processed[[
                    "Title", "Text", "Link", "Date", "Tickers_found", "sentiment"
                ]])
            )
        ][0]

        self.tele_data_to_upload = [
            {"channel": channel,
             "date": date,
             "sender": sender,
             "message": message,
             "tickers": json.loads(tickers),
             "sentiments": json.loads(sentiments)}
            for channel, date, sender, message, tickers, sentiments in zip(
                *self.splitter(self.tele_data_processed[[
                    "channel", "date", "sender", "message", "Tickers_found", "sentiments"
                ]])
            )
        ][0]

    def upload_to_firestore(self):
        self.firestoreDB_layer("SBR_data", self.SBR_data_to_upload)
        print("SBR Data successfully uploaded.")
        self.firestoreDB_layer("Tele_data", self.tele_data_to_upload)
        print("Tele Data successfully uploaded.")

    # Utility Functions

    def splitter(self, df):
        cols = list(df.columns)
        output = []
        for col in cols:
            output.append(df[col])
        return output
