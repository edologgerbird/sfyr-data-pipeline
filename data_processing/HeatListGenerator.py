'''
TIcker Heatlist Generator API
Input: DataFrame with Columns: [str(Text), dict(Tickers), float(postive), float(negative), float(neutral)]
Input: {'tickers': [], 'sentiments': {
    'positive':float, 'negative':float, 'neutral':float}}
Output: Frequency distribution of Tickers
'''
import pandas as pd
import json
import numpy as np


class HeatListGenerator:
    def __init__(self):
        # self.df = df.rename(columns={df.columns[0]: "text", df.columns[1]: "tickers",
        #                     df.columns[2]: "positive", df.columns[3]: "negative", df.columns[4]: "neutral"})

        self.sgx_data = pd.read_csv(
            "csv_store/SGX_data.csv"
        )
        self.sgx_data_mapper = {x: y for x, y in zip(
            self.sgx_data["company_code"], self.sgx_data["company_name"])}

        self.industry_df = pd.read_csv(
            "csv_store/industry_new.csv")  # Replace with GBQ query
        self.industry_mapper = {x: y for x, y in zip(
            self.industry_df["company_code"], self.industry_df["industry"])}

        self.ticker_heat_list = dict()
        self.industry_heat_list = dict()
        self.frequency_counter = dict()
        self.tickers_present = dict()

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
        for company_code in ticker_list:
            if company_code not in self.ticker_heat_list:
                self.ticker_heat_list[company_code] = dict_res["sentiments"]["postive"] - \
                    dict_res["sentiments"]["negative"]
                self.frequency_counter[company_code] = 1
                self.tickers_present[company_code] = self.sgx_data_mapper[company_code]
            else:
                self.ticker_heat_list[company_code] += (
                    dict_res["sentiments"]["postive"] - dict_res["sentiments"]["negative"])
                self.frequency_counter[company_code] += 1

            if company_code in self.industry_mapper and self.industry_mapper[company_code] is not np.NaN:
                industry = self.industry_mapper[company_code]
                if self.industry_mapper[company_code] not in self.industry_heat_list:
                    self.industry_heat_list[industry] = dict_res["sentiments"]["postive"] - \
                        dict_res["sentiments"]["negative"]
                else:
                    self.industry_heat_list[industry] += dict_res["sentiments"]["postive"] - \
                        dict_res["sentiments"]["negative"]

    def generateHeatList(self, dict_query):
        self.dict_query = dict_query
        for res in dict_query:
            self.generateHeatScoreFromRes(res)
        #self.df.apply(lambda x: self.generateHeatScoreFromRow(x), axis=1)
        ticker_heat_list, industry_heat_list = self.getHeatListNormalised()
        ticker_heat_list.reset_index(inplace=True)
        ticker_heat_list = ticker_heat_list.rename(columns={'index': 'ticker'})
        ticker_heat_list["company"] = ticker_heat_list['ticker'].apply(
            lambda x: self.tickers_present[x])

        industry_heat_list.reset_index(inplace=True)
        industry_heat_list = industry_heat_list.rename(
            columns={"index": "industry"})

        return ticker_heat_list, industry_heat_list


# Test
# test_csv = pd.read_csv("csv_store/TickerHeatlistInput.csv")
# HeatListGenerator_Layer = HeatListGenerator(test_csv)
# ticker_heat_list, industry_heat_list = HeatListGenerator_Layer.generateHeatList()
# ticker_heat_list.to_csv("csv_store/ticker_heat_list.csv")
# industry_heat_list.to_csv("csv_store/industry_heat_list.csv")
