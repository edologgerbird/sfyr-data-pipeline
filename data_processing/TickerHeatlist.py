'''
TIcker Heatlist Generator API
Input: DataFrame with Columns: [str(Text), dict(Tickers), float(postive), float(negative), float(neutral)]
Output: Frequency distribution of Tickers
'''
import pandas as pd
from collections import Counter
import json


class TickerHeatList:
    def __init__(self, df):
        self.df = df.rename(columns={df.columns[0]: "text", df.columns[1]: "tickers",
                            df.columns[2]: "positive", df.columns[3]: "negative", df.columns[4]: "neutral"})
        self.heat_list = dict()
        self.frequency_counter = dict()
        self.tickers_present = dict()

    def getTickerFreq(self):
        df = pd.DataFrame(self.frequency_counter, index=[
                          "count"]).T.sort_values("count", ascending=False)
        return df

    def getHeatList(self):
        df = pd.DataFrame(self.heat_list, index=["heat"]).T.sort_values(
            "heat", ascending=False)
        return df

    def getHeatListNormalised(self):
        df = self.getHeatList()
        df["heat"] = (df["heat"]-df["heat"].min()) / \
            (df["heat"].max()-df["heat"].min())
        return df

    def getTickersPresent(self):
        df = pd.DataFrame(self.tickers_present, index=[
                          "ticker_name"]).T.sort_values("ticker_name")
        return df

    def generateHeatScoreFromRow(self, row):
        ticker_dict = json.loads(row["tickers"].replace("\'", "\""))
        for company_code, company_name in ticker_dict.items():
            if company_code not in self.heat_list:
                self.heat_list[company_code] = row["positive"] - \
                    row["negative"]
                self.frequency_counter[company_code] = 1
                self.tickers_present[company_code] = company_name
            else:
                self.heat_list[company_code] += (
                    row["positive"] - row["negative"])
                self.frequency_counter[company_code] += 1

    def generateHeatList(self):
        self.df.apply(lambda x: self.generateHeatScoreFromRow(x), axis=1)
        output = self.getHeatListNormalised()
        output.reset_index(inplace=True)
        output = output.rename(columns={'index': 'ticker'})
        output["company"] = output['ticker'].apply(
            lambda x: self.tickers_present[x])

        return output


# Test

test_csv = pd.read_csv("csv_store/TickerHeatlistInput.csv")
TickerHeatList_Layer = TickerHeatList(test_csv)
TickerHeatList_Layer.generateHeatList().to_csv("test_heatlist.csv")
