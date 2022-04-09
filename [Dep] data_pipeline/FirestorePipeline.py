'''
Firestore Data Pipeline
1. Executer for Source Pipelines
'''

import numpy as np
import pandas as pd
import json
from datetime import datetime as dt

from data_load.firestoreAPI import firestoreDB
from data_pipeline.SBRNewsPipeline import SBRNewsPipeline
from data_pipeline.telegramNewsPipeline import telegramNewsPipeline

from data_transform.TickerExtractor import TickerExtractor
from data_processing.FinBertAPI import FinBERT


class FirestorePipeline:
    def __init__(self):
        print("Initialising Firestore Pipeline...")
        self.firestoreDB_layer = firestoreDB()
        self.ticker_extractor_layer = TickerExtractor()
        self.FinBERT_layer = FinBERT()
        self.SBRNewsPipeline_layer = SBRNewsPipeline(
            self.firestoreDB_layer, self.ticker_extractor_layer, self.FinBERT_layer)
        self.telegramNewsPipeline_layer = telegramNewsPipeline(
            self.firestoreDB_layer, self.ticker_extractor_layer, self.FinBERT_layer)

        # insert more layers below:

        print("Firestore Pipeline Initialised")

    def execute_pipeline(self, start_date, end_date):
        self.SBRNewsPipeline_layer.execute_pipeline(start_date, end_date)
        self.telegramNewsPipeline_layer.execute_pipeline(start_date, end_date)
