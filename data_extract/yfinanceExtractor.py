from datetime import datetime as dt
import pandas as pd
import numpy as np
import yfinance as yf
import time
import json
from functools import reduce


class yfinanceExtractor:
    def __init__(self, sgxTickers):
        self.sgxTickers = sgxTickers
        self.sgxTickers.ticker = self.sgxTickers.ticker.str[:] + ".SI"

        # Initalisation of yFinance Schema Datafile
        self.yfinanceColumnNameUrl = "data_extract/yfinanceColumnName.json"
        with open(self.yfinanceColumnNameUrl, 'r') as jsonFile:
            self.ColumnName = json.load(jsonFile)

        # Initalisation of Shared Data
        self.ticker_active = []  # List of Active Ticker Objects
        self.ticker_delisted = []  # List of Inactive Ticker Name String

        # Initalise yFinance Output
        self.yfinanceData = {}
        for dataField in self.ColumnName["columnNames"]:
            self.yfinanceData[dataField] = pd.DataFrame(
                columns=self.ColumnName["columnNames"][dataField])

        # Check on Ticker Active/Inactive

        self.checkTickers()
        print("INFO: yFinanceExtractor Initialised")

    def checkTickers(self):
        print("INFO: Obtaining active tickers")
        activeTickers = []
        delisted = []

        # Check if ticker exist
        no_of_tickers = len(self.sgxTickers["ticker"])
        counter = 1
        for ticker in self.sgxTickers["ticker"]:
            print(
                f">> ========== Extracting {ticker} Overall Progress: {counter}/{no_of_tickers}")
            time.sleep(0.23)
            length = yf.download(
                ticker, period='max', interval='1d', timeout=None).shape[0]
            if length > 0:
                activeTickers.append(ticker)
            else:
                delisted.append(ticker)
            counter += 1

        # Generate dataframe of existing and missing tickers
        dfTickers = pd.DataFrame({'Listed': activeTickers})
        dfTickers["Delisted"] = pd.Series(delisted)

        # Update Class Copy
        self.yfinanceData["ticker_status"] = dfTickers
        self.ticker_active = [yf.Ticker(ticker) for ticker in activeTickers]
        self.ticker_delisted = delisted
        print("SUCCESS: Active tickers obtained")
        return dfTickers

    # ---- Helper Functions  ----- #
    def removeSI(self, ticker):
        return ticker[:-3]

    def cast_dict_to_string(self, entry):
        if len(str(entry)) > 0 and str(entry)[0] in ["{", "["]:
            return str(entry)
        else:
            return entry

    # ---- Main Functions  ----- #
    def getHistoricalData(self, start_date=dt.now()):
        target_data = "historical_data"
        print(f"INFO: Extracting {target_data}")
        # Listed Tickers' historical market data
        historical_data_df = self.yfinanceData[target_data]
        counter = 1
        for ticker in self.ticker_active:
            print(
                f"INFO: {target_data} - Extracting {ticker} | Progress: ticker {counter}/{len(self.ticker_active)}")
            if start_date is None:
                tickerHistoricalData=yf.download(
                    ticker.ticker, period = 'max', interval = '1d', timeout = None)
                tickerHistoricalData["Tickers"]=self.removeSI(ticker.ticker)
                tickerHistoricalData["Market Status"]="Market_Closed"

            else:
                start_date_string=start_date.strftime('%Y-%m-%d')
                tickerHistoricalData=yf.download(
                    ticker.ticker, start = start_date_string, interval = '1d', timeout = None)
                tickerHistoricalData["Tickers"]=self.removeSI(ticker.ticker)

                if int(start_date.hour) < 12:
                    tickerHistoricalData["Market Status"]="Market_Open"
                else:
                    tickerHistoricalData["Market Status"]="Market_Closed"

            counter += 1

            historical_data_df=pd.concat(
                [historical_data_df, tickerHistoricalData])

        historical_data_df=historical_data_df.reset_index()

        # Store to Shared Data
        self.yfinanceData[target_data]=historical_data_df
        print(f"SUCCESS: {target_data} successfully extracted")
        return historical_data_df

    def getFinancialStatement(self):
        target_data="financial_statements"
        print("INFO: Extracting {target_data}")
        financial_statements_df=self.yfinanceData[target_data]
        counter=1
        for ticker in self.ticker_active:
            print(
                f"INFO: {target_data} - Extracting {ticker} | Progress: ticker {counter}/{len(self.ticker_active)}")
            profit_and_loss=ticker.financials.T.rename_axis(
                'Date').reset_index()

            balance_sheet=ticker.balance_sheet.T.rename_axis(
                'Date').reset_index()

            cashflow=ticker.cashflow.T.rename_axis(
                'Date').reset_index()

            financial_statements=reduce(lambda left_table, right_table: pd.merge(
                left_table, right_table, on='Date'), [profit_and_loss, balance_sheet, cashflow])
            financial_statements = financial_statements.fillna(np.NaN)
            if type(financial_statements["Date"].values[0]) != str:
                financial_statements_df = pd.concat(
                    [financial_statements_df, financial_statements])

            counter += 1
        self.yfinanceData[target_data]=financial_statements_df
        print("SUCCESS: {target_data} successfully extracted")
        return financial_statements_df

    def getQuarterlyFinancialStatement(self):
        target_data="quarterly_financial_statements"
        print(f"INFO: Extracting {target_data}")
        quarterly_financial_statements_df=self.yfinanceData[target_data]
        counter=1
        for ticker in self.ticker_active:
            print(
                f"INFO: {target_data} - Extracting {ticker} | Progress:{counter}/{len(self.ticker_active)}")
            profit_and_loss=ticker.quarterly_financials.T.rename_axis(
                'Date').reset_index()

            balance_sheet=ticker.quarterly_balancesheet.T.rename_axis(
                'Date').reset_index()

            cashflow=ticker.quarterly_cashflow.T.rename_axis(
                'Date').reset_index()

            financial_statements=reduce(lambda left_table, right_table: pd.merge(
                left_table, right_table, on='Date'), [profit_and_loss, balance_sheet, cashflow])
            financial_statements = financial_statements.fillna(np.NaN)
            if type(financial_statements["Date"].values[0]) != str:
                quarterly_financial_statements_df = pd.concat(
                    [quarterly_financial_statements_df, financial_statements])

            counter += 1
        self.yfinanceData[target_data]=quarterly_financial_statements_df
        print(f"SUCCESS: {target_data} successfully extracted")
        return quarterly_financial_statements_df

    def getEarningsandRevenue(self):
        target_data="earnings_and_revenue"
        print(f"INFO: Extracting {target_data}")
        # Get Earnings and Revenue
        earnings_and_revenues_df=self.yfinanceData[target_data]
        counter=1
        for ticker in self.ticker_active:
            print(
                f"INFO: {target_data} - Extracting {ticker} | Progress:{counter}/{len(self.ticker_active)}")
            if (ticker.earnings.shape[0] >= 1):
                ticker_earning_and_revenue=ticker.earnings
                ticker_earning_and_revenue['Tickers']=self.removeSI(
                    ticker.ticker)
                earnings_and_revenues_df=pd.concat(
                    [earnings_and_revenues_df, ticker_earning_and_revenue])
            counter += 1
        earnings_and_revenues_df=earnings_and_revenues_df.reset_index().rename(columns = {
            'index': 'Year'})

        # Store to Shared Data
        self.yfinanceData[target_data]=earnings_and_revenues_df
        print(f"SUCCESS: {target_data} successfully extracted")
        return earnings_and_revenues_df

    def getQuarterlyEarningsandRevenue(self):
        target_data="quarterly_earnings_and_revenue"
        print(f"INFO: Extracting {target_data}")
        # Get Quarterly Earnings and Revenue
        quarterly_earnings_and_revenues_df=self.yfinanceData[target_data]
        counter=1
        for ticker in self.ticker_active:
            print(
                f"INFO: {target_data} - Extracting {ticker} | Progress:{counter}/{len(self.ticker_active)}")
            if (ticker.quarterly_earnings.shape[0] >= 1):
                ticker_quarterly_earning_and_revenue=ticker.quarterly_earnings
                ticker_quarterly_earning_and_revenue['Tickers']=self.removeSI(
                    ticker.ticker)
                quarterly_earnings_and_revenues_df=pd.concat(
                    [quarterly_earnings_and_revenues_df, ticker_quarterly_earning_and_revenue])
            counter += 1
        quarterly_earnings_and_revenues_df=quarterly_earnings_and_revenues_df.reset_index(
        )

        first_col_name = list(quarterly_earnings_and_revenues_df.columns)[0]

        quarterly_earnings_and_revenues_df = quarterly_earnings_and_revenues_df.rename(
            columns={first_col_name: 'Quarters'})

        quarterly_earnings_and_revenues_df['Quarters'] = quarterly_earnings_and_revenues_df['Quarters'].astype(
            str)

        # Store to Shared Data
        self.yfinanceData[target_data] = quarterly_earnings_and_revenues_df
        print(f"SUCCESS: {target_data} successfully extracted")
        return quarterly_earnings_and_revenues_df

    def getMajorHolders(self):
        # Get Major Holders
        target_data = "majorHolders"
        print(f"INFO: Extracting {target_data}")
        majorHolders_df = self.yfinanceData[target_data]
        counter = 1
        for ticker in self.ticker_active:
            print(
                f"INFO: {target_data} - Extracting {ticker} | Progress:{counter}/{len(self.ticker_active)}")

            if ticker.major_holders is not None and ticker.major_holders.shape[0] == 4:
                ticker_majorHolders = ticker.major_holders.set_index(1)
                ticker_majorHolders = ticker_majorHolders.T
                ticker_majorHolders["Tickers"] = self.removeSI(ticker.ticker)
                ticker_majorHolders[:] = ticker_majorHolders[:].astype(
                    'string')

                majorHolders_df = majorHolders_df.merge(
                    ticker_majorHolders, how="outer", on=list(ticker_majorHolders.columns))
            counter += 1
        self.yfinanceData[target_data] = majorHolders_df
        print(f"SUCCESS: {target_data} successfully extracted")
        return majorHolders_df

    def getBasicShares(self):
        # Get Basic Shares
        target_data = "basic_shares"
        print(f"INFO: Extracting {target_data}")
        basic_shares_df = self.yfinanceData[target_data]
        counter = 1
        for ticker in self.ticker_active:
            print(
                f"INFO: {target_data} - Extracting {ticker} | Progress:{counter}/{len(self.ticker_active)}")
            if ticker.shares is not None:
                ticker_share = ticker.shares
                ticker_share['Tickers'] = self.removeSI(ticker.ticker)
                basic_shares_df = pd.concat(
                    [basic_shares_df, ticker_share])
            counter += 1
        basic_shares_df = basic_shares_df.reset_index()

        basic_shares_df = basic_shares_df.rename(
            columns={basic_shares_df.columns[0]: "Year"})
        self.yfinanceData[target_data] = basic_shares_df
        print(f"SUCCESS: {target_data} successfully extracted")
        return basic_shares_df

    def getStockInfo(self):
        # Get stock information
        target_data = "stock_info"
        print(f"INFO: Extracting {target_data}")
        all_tickers_info = self.yfinanceData[target_data]
        counter = 1
        for ticker in self.ticker_active:
            print(
                f"INFO: {target_data} - Extracting {ticker} | Progress:{counter}/{len(self.ticker_active)}")
            if ticker.info is not None:
                ticker_info = pd.DataFrame(ticker.info.items()).set_index(0).T
                ticker_info["Tickers"] = self.removeSI(ticker.ticker)
                ticker_info["Tickers"] = ticker_info["Tickers"].astype(
                    "string")
                all_tickers_info = pd.concat([all_tickers_info, ticker_info])
            counter += 1
        all_tickers_info = all_tickers_info.reset_index(drop=True)

        for column in all_tickers_info.columns:
            all_tickers_info[column] = all_tickers_info[column].apply(
                self.cast_dict_to_string)

        self.yfinanceData[target_data] = all_tickers_info
        print(f"SUCCESS: {target_data} successfully extracted")
        return all_tickers_info

    def getStockIndustry(self):
        target_data = "stock_industry"
        print(f"INFO: Extracting {target_data}")
        if self.yfinanceData["stock_info"].empty:
            self.getStockInfo()
        stock_info = self.yfinanceData["stock_info"]
        stock_industry = stock_info[["Tickers", "industry"]]
        stock_industry = stock_industry.rename(columns={"Tickers": "ticker"})

        # Store to Shared Data
        self.yfinanceData[target_data] = stock_industry
        print(f"SUCCESS: {target_data} successfully extracted")
        return stock_industry

    def getCalendar(self):
        # Get next event (earnings, etc)
        target_data = "stock_calendar"
        print(f"INFO: Extracting {target_data}")
        stock_calendar_df = self.yfinanceData[target_data]
        counter = 1
        for ticker in self.ticker_active:
            print(
                f"INFO: {target_data} - Extracting {ticker} | Progress:{counter}/{len(self.ticker_active)}")
            if ticker.calendar is not None:
                ticker_calendar = ticker.calendar.transpose()
                ticker_calendar = ticker_calendar.rename(
                    columns={"Ticker": "Tickers"})
                ticker_calendar['Tickers'] = self.removeSI(ticker.ticker)
                stock_calendar_df = pd.concat(
                    [stock_calendar_df, ticker_calendar])
            counter += 1
        stock_calendar_df = stock_calendar_df.reset_index(drop=True)
        stock_calendar_df.replace({np.nan: np.nan}, inplace=True)

        # Store to Shared Data
        self.yfinanceData[target_data] = stock_calendar_df
        print(f"SUCCESS: {target_data} successfully extracted")
        return stock_calendar_df

    def getRecommendations(self):
        # Get Recommendations
        target_data = "stock_recommendation"
        print(f"INFO: Extracting {target_data}")
        recommendations_df = self.yfinanceData[target_data]
        counter = 1
        for ticker in self.ticker_active:
            print(
                f"INFO: {target_data} - Extracting {ticker} | Progress:{counter}/{len(self.ticker_active)}")
            if ticker.recommendations is not None:
                ticker_recommendations = ticker.recommendations
                ticker_recommendations['Tickers'] = self.removeSI(
                    ticker.ticker)
                ticker_recommendations["Date"] = ticker_recommendations.index
                recommendations_df = pd.concat(
                    [recommendations_df, ticker_recommendations])
            counter += 1
        recommendations_df = recommendations_df.reset_index(drop=True)

        # Store to Shared Data
        self.yfinanceData[target_data] = recommendations_df
        print(f"SUCCESS: {target_data} successfully extracted")
        return recommendations_df

    def getAnalysis(self):
        # Get Analysis
        target_data = "stock_analysis"
        print(f"INFO: Extracting {target_data}")
        analysis_df = self.yfinanceData[target_data]
        counter = 1
        for ticker in self.ticker_active:
            print(
                f"INFO: {target_data} - Extracting {ticker} | Progress:{counter}/{len(self.ticker_active)}")
            if ticker.analysis is not None:
                ticker_analysis = ticker.analysis
                ticker_analysis['Tickers'] = self.removeSI(ticker.ticker)
                ticker_analysis['Tickers'] = ticker_analysis['Tickers'].astype(
                    "string")
                analysis_df = pd.concat(
                    [analysis_df, ticker_analysis])
            counter += 1

        analysis_df = analysis_df.reset_index().rename(columns={
            'index': 'Period'})
        analysis_df['Period'] = analysis_df['Period'].astype(str)

        # Store to Shared Data
        self.yfinanceData[target_data] = analysis_df
        print(f"SUCCESS: {target_data} successfully extracted")
        return analysis_df

    def getMutualFundHolders(self):
        # Get Mutual Fund Holders
        target_data = "stock_mfh"
        print(f"INFO: Extracting {target_data}")
        mfh_pd = self.yfinanceData[target_data]
        counter = 1
        for ticker in self.ticker_active:
            print(
                f"INFO: {target_data} - Extracting {ticker} | Progress:{counter}/{len(self.ticker_active)}")
            if ticker.mutualfund_holders is not None:
                ticker_mfh = ticker.mutualfund_holders
                ticker_mfh['Tickers'] = self.removeSI(ticker.ticker)
                mfh_pd = pd.concat([mfh_pd, ticker_mfh])
            counter += 1

        mfh_pd = mfh_pd.reset_index(drop=True)

        # Store to Shared Data
        self.yfinanceData[target_data] = mfh_pd
        print(f"SUCCESS: {target_data} successfully extracted")
        return mfh_pd

    def getInstitutionalHolders(self):
        target_data = "stock_ih"
        # Get Institutional holders
        print(f"INFO: Extracting {target_data}")
        ih_pd = self.yfinanceData[target_data]
        counter = 1
        for ticker in self.ticker_active:
            print(f"INFO: {target_data} - Extracting {ticker} | Progress:{counter}/{len(self.ticker_active)}")
            if ticker.institutional_holders is not None and ticker.institutional_holders.shape[1] == 5:
                ticker_ih=ticker.institutional_holders
                ticker_ih['Tickers']=self.removeSI(ticker.ticker)
                ih_pd=pd.concat([ih_pd, ticker_ih])
            counter += 1
        ih_pd=ih_pd.reset_index(drop = True)

        # Store to Shared Data
        self.yfinanceData[target_data]=ih_pd
        print(f"SUCCESS: {target_data} successfully extracted")
        return ih_pd

    def yfinanceQuery(self):
        failed=[]
        query_calls={
            "Historical Data": self.getHistoricalData,
            "Financial Statement": self.getFinancialStatement,
            "Quarterly Financial Statement": self.getQuarterlyFinancialStatement,
            "Earnings and Revenue": self.getEarningsandRevenue,
            "Quarterly Earnings and Revenue": self.getQuarterlyEarningsandRevenue,
            "Major Holders": self.getMajorHolders,
            "Basic Shares": self.getBasicShares,
            "Stock Info": self.getStockInfo,
            "Stock Industry": self.getStockIndustry,
            "Calendar": self.getCalendar,
            "Analyst Recommendations": self.getRecommendations,
            "Stock Analysis": self.getAnalysis,
            "Mutual Fund Holders": self.getMajorHolders,
            "Institutional Holders": self.getInstitutionalHolders,
        }

        for query, query_fn in query_calls.items():
            try:
                query_fn()
            except:
                print(f"ERROR {query} failed to extract")
                failed.append(query)

        return self.yfinanceData
