'''
Util Functions
'''
from datetime import datetime as dt


def splitter(df):
    return [df[col] for col in list(df.columns)]


def string_to_date(date, delimiter):
    return dt.strptime(date.split(delimiter)[0], "%Y-%m-%d")
