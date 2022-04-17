'''
TIcker Heatlist Generator API
Input: DataFrame with Columns: [str(Text), dict(Tickers), float(postive), float(negative), float(neutral)]
Input: {'tickers': [], 'sentiments': {
    'positive':float, 'negative':float, 'neutral':float}}
Output: Frequency distribution of Tickers
'''
import pandas as pd
import numpy as np


class HeatListGenerator:
    def __init__(self, sgx_data, industry_df):
        print("INFO: Initialising Heat List Generator")
        self.datasetTable = "SGX.Tickers"
        self.sgx_data = sgx_data
        self.sgx_data_mapper = {x: y for x, y in zip(
            self.sgx_data["ticker"], self.sgx_data["company_name"])}

        self.industry_df = industry_df
        self.industry_mapper = {x: y for x, y in zip(
            self.industry_df["ticker"], self.industry_df["industry"])}

        self.ticker_heat_list = dict()
        self.industry_heat_list = dict()
        self.frequency_counter = dict()
        self.tickers_present = dict()
        print("INFO: Heat List Generator Initialised")

    def getTickerFreq(self):
        df = pd.DataFrame(self.frequency_counter, index=[
                          "count"]).T.sort_values("count", ascending=False)
        return df

    def getTickerHeatList(self):
        df = pd.DataFrame(self.ticker_heat_list, index=["heat"]).T.sort_values(
            "heat", ascending=False)
        return df

    def getIndustryHeatList(self):
        df = pd.DataFrame(self.industry_heat_list, index=["heat"]).T.sort_values(
            "heat", ascending=False)
        return df

    def normaliseColumn(self, col):
        output = (col-col.mean())/col.std()
        return output

    def getHeatListNormalised(self):
        ticker_heat_list = self.getTickerHeatList()
        ticker_heat_list["heat"] = self.normaliseColumn(
            ticker_heat_list["heat"])
        industry_heat_list = self.getIndustryHeatList()
        industry_heat_list["heat"] = self.normaliseColumn(
            industry_heat_list["heat"])
        return ticker_heat_list, industry_heat_list

    def getTickersPresent(self):
        df = pd.DataFrame(self.tickers_present, index=[
                          "ticker_name"]).T.sort_values("ticker_name")
        return df

    def generateHeatScoreFromRes(self, dict_res):
        ticker_list = [ticker for sublist in [x["tickers"]
                                              for x in self.dict_query] for ticker in sublist]
        for ticker in ticker_list:
            if ticker not in self.ticker_heat_list:
                self.ticker_heat_list[ticker] = dict_res["sentiments"]["positive"] - \
                    dict_res["sentiments"]["negative"]
                self.frequency_counter[ticker] = 1
                if ticker in self.sgx_data_mapper:
                    self.tickers_present[ticker] = self.sgx_data_mapper[ticker]
                else:
                    self.tickers_present[ticker] = None
            else:
                self.ticker_heat_list[ticker] += (
                    dict_res["sentiments"]["positive"] - dict_res["sentiments"]["negative"])
                self.frequency_counter[ticker] += 1

            if ticker in self.industry_mapper and self.industry_mapper[ticker] is not np.NaN:
                industry = self.industry_mapper[ticker]
                if self.industry_mapper[ticker] not in self.industry_heat_list:
                    self.industry_heat_list[industry] = dict_res["sentiments"]["positive"] - \
                        dict_res["sentiments"]["negative"]
                else:
                    self.industry_heat_list[industry] += dict_res["sentiments"]["positive"] - \
                        dict_res["sentiments"]["negative"]

    def generateHeatList(self, dict_query):
        print("INFO: Generating Heat List")
        self.dict_query = dict_query
        for res in dict_query:
            self.generateHeatScoreFromRes(res)
        ticker_heat_list, industry_heat_list = self.getHeatListNormalised()
        ticker_heat_list.reset_index(inplace=True)
        ticker_heat_list = ticker_heat_list.rename(columns={'index': 'ticker'})
        ticker_heat_list["company"] = ticker_heat_list['ticker'].apply(
            lambda x: self.tickers_present[x])

        industry_heat_list.reset_index(inplace=True)
        industry_heat_list = industry_heat_list.rename(
            columns={"index": "industry"})
        print("SUCCESS: Heat Lists generated")
        return ticker_heat_list, industry_heat_list
