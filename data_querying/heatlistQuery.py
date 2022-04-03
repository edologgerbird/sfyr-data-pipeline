'''
Querying Module for Heatlist

'''
import datetime as dt
from dateutil.parser import parse

class HeatListQuery:
    def __init__(self, firestoreDB):
        self.firestoreDB_layer = firestoreDB
        self.documents_container = list()
        self.start_date = None
        self.end_date = None

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

    def query_documents_by_date(self, collection):
        if not self.start_date:
            raise Exception("Start date not set!")
        if not self.end_date:
            raise Exception("End date not set!")

        query_start_date = ["date", ">", self.start_date]
        query_end_date = ["date", "<", self.end_date]

        query_results = self.firestoreDB_layer.fsQueryDocuments(
            collection, query_start_date, query_end_date)
        self.documents_container += query_results

        return self.documents_container


