
'''
ETL for SBR Data, from Source to Firestore
'''

import numpy as np
import pandas as pd
import json
from datetime import datetime as dt

from data_load.firestoreAPI import firestoreDB
from data_processing.FinBertAPI import FinBERT
from data_transform.TickerExtractor import TickerExtractor
from data_transform.STIMovementExtractor import STIMovementExtractor
from data_extract.SBRExtractor import SBRExtractor


class FirestorePipeline:
    def __init__(self):
        print("Initialising Firestore Pipeline...")
        self.SBR_data_extractor_layer = SBRExtractor()
        self.ticker_extractor_layer = TickerExtractor()
        self.STI_movement_extractor_layer = STIMovementExtractor()
        self.FinBERT_layer = FinBERT()
        self.firestoreDB_layer = firestoreDB()

        self.SBR_data_raw = None

        self.SBR_data_processed = None

        self.SBR_data_to_upload = None
        print("SBR Pipeline Initialised")

    # date format: "dd-mm-yyyy"
    def extract_data_from_source(self, start_date, end_date):
        # Extract from SBR
        self.SBR_data_raw = self.SBR_data_extractor_layer.load_SBR_data_from_source(
            start_date=start_date, end_date=end_date)

    def transform_and_process_data(self):
        # Ticker Extraction for SBR Data
        SBR_data_with_tickers = self.ticker_extractor_layer.populate_ticker_occurences(
            self.SBR_data_raw["Title"] + " " + self.SBR_data_raw["Text"])

        # STI Movement Extraction for SBR Data
        SBR_data_with_tickers[['STI_direction', 'STI_movement']] = self.STI_movement_extractor_layer.populate_sti_movement(
            self.SBR_data_raw['Text'])[['Direction of STI Movement', 'Percentage of STI Movement']]

        # NLP for SBR Data sentiments
        SBR_data_with_sentiments = self.FinBERT_layer.FinBert_pipeline(
            self.SBR_data_raw["Title"] + " " + self.SBR_data_raw["Text"])

        # # Combining Dataframes
        SBR_data_with_tickers["sentiment"] = [{"sentiment": {
            "positive": x, "negative": y, "neutral": z}}for x, y, z in zip(
            *self.splitter(SBR_data_with_sentiments[["Positive", "Negative", "Neutral"]])
        )]

        # self.SBR_data_processed = SBR_data_with_tickers
        self.SBR_data_processed = SBR_data_with_tickers
        self.SBR_data_processed[["Date", "Title", "Text", "Link"]
                                ] = self.SBR_data_raw[["Date", "Title", "Text", "Link"]]

        # Transforming Data to NoSQL format

        # SBR Data
        self.SBR_data_to_upload = [
            {"text_headline": headline,
             "text_body": body,
             "link": link,
             "date": date,
             "tickers": [ticker for ticker in tickers.keys()],
             "sti_movement": {"direction": direction, "amount": amount},
             "sentiments": list(sentiments.values())[0]}
            for headline, body, link, date, tickers, direction, amount, sentiments in zip(
                *self.splitter(self.SBR_data_processed[[
                    "Title", "Text", "Link", "Date", "Tickers_found", "STI_direction", "STI_movement", "sentiment"
                ]])
            )
        ]

    def upload_to_firestore(self):
        self.firestoreDB_layer.fsAddListofDocuments(
            "SBR_data", self.SBR_data_to_upload)
        print("SBR Data successfully uploaded.")

    def execute_pipeline(self, start_date, end_date):
        self.extract_data_from_source(start_date, end_date)
        self.transform_and_process_data()
        self.upload_to_firestore()

    # Utility Functions

    def splitter(self, df):
        return [df[col] for col in list(df.columns)]

    def string_to_date(self, date, delimiter):
        return dt.strptime(date.split(delimiter)[0], "%Y-%m-%d")
