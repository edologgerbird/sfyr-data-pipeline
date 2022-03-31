'''
Pipeline for Heatlist
- Query from firebase
- Input into HeatListGenerator
- Output to BigQuery

'''
from tracemalloc import start
from data_load.firestoreAPI import firestoreDB
from data_processing.HeatListGenerator import HeatListGenerator

import json
import numpy as np
import pandas as pd
import datetime as dt
from dateutil.parser import parse


class HeatListPipeline:
    def __init__(self):
        self.firestoreDB_layer = firestoreDB()
        self.HeatListGenerator_layer = HeatListGenerator()
        self.documents_container = list()

        self.start_date = None
        self.end_date = None

        self.collections = ['SBR_data', 'Telegram_data', "Yahoo-Fin-News"]

        self.ticker_heatlist = None
        self.industry_heatlist = None

    def get_look_back_period(self, end_date, no_look_back_days):
        end_date = parse(end_date, dayfirst=True)
        start_date = end_date + dt.timedelta(days=-no_look_back_days)
        return start_date, end_date

    def set_dates(self, start_date=None, end_date=None):
        self.start_date = start_date + \
            dt.timedelta(days=-1) if (start_date is not None) else None
        self.end_date = end_date + dt.timedelta(days=1)

        if (self.start_date is not None and self.end_date is not None and self.start_date > self.end_date):
            raise Exception('Start date input must be before end date input')

    def query_documents_by_date(self):
        if not self.start_date:
            raise Exception("Start date not set!")
        if not self.end_date:
            raise Exception("End date not set!")

        query_start_date = ["date", ">", self.start_date]
        query_end_date = ["date", "<", self.end_date]

        for collection in self.collections:
            query_results = self.firestoreDB_layer.fsQueryDocuments(
                collection, query_start_date, query_end_date)
            self.documents_container += query_results

        return self.documents_container

    def get_heatlist_data_from_query(self, query_res):
        heatlist_fields = ["tickers", "sentiments"]
        subset = {k: query_res[k] for k in heatlist_fields}
        return subset

    def query_pipeline(self, date):
        start_date, end_date = self.get_look_back_period(date, 7)
        self.set_dates(start_date=start_date, end_date=end_date)
        query_results = self.query_documents_by_date()
        output = [self.get_heatlist_data_from_query(
            query_result) for query_result in query_results]
        return output

    def generate_heatlist(self, query_results):
        ticker_heatlist, industry_heatlist = self.HeatListGenerator_layer.generateHeatList(
            query_results)
        return ticker_heatlist, industry_heatlist

    def HeatlistPipeline_execute(self, date):
        query_results = self.query_pipeline(date)
        self.ticker_heatlist, self.industry_heatlist = self.generate_heatlist(
            query_results)
        # upload to GBQ here
        return self.ticker_heatlist, self.industry_heatlist
