'''
Util Functions
'''
from datetime import datetime, timedelta
import datetime as dt
import pandas as pd
import pendulum


def splitter(df):
    return [df[col] for col in list(df.columns)]


def string_to_date(date, delimiter):
    return dt.strptime(date.split(delimiter)[0], "%Y-%m-%d")


def get_execute_time(date):
    date_plus_one = datetime.now() + timedelta(days=1)
    date_day = date_plus_one.day
    date_month = date_plus_one.month
    date_year = date_plus_one.year
    date_excute_time = pendulum.datetime(
        year=date_year, month=date_month, day=date_day, hour=9, minute=30, tz="Asia/Singapore")
    return date_excute_time


def get_extraction_schedule(curr_time):
    if curr_time.hour < 12:
        extraction_start_date = pendulum.datetime(
            year=curr_time.year, month=curr_time.month, day=curr_time.day, hour=21, minute=30, tz="Asia/Singapore") - timedelta(days=1)
        extraction_end_date = pendulum.datetime(
            year=curr_time.year, month=curr_time.month, day=curr_time.day, hour=9, minute=30, tz="Asia/Singapore")
    else:
        extraction_start_date = pendulum.datetime(
            year=curr_time.year, month=curr_time.month, day=curr_time.day, hour=9, minute=30, tz="Asia/Singapore")
        extraction_end_date = pendulum.datetime(
            year=curr_time.year, month=curr_time.month, day=curr_time.day, hour=21, minute=30, tz="Asia/Singapore")

    return extraction_start_date, extraction_end_date
