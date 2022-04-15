from datetime import datetime as dt
import pandas as pd
import numpy as np
import yfinance as yf
import time
from functools import reduce


class yfinanceExtractor:
    def __init__(self, sgxTickers):
        self.sgxTickers = sgxTickers
        self.sgxTickers.ticker = self.sgxTickers.ticker.str[:] + ".SI"

        # Initalisation of Shared Data
        self.ticker_active = []  # List of Active Ticker Objects
        self.ticker_delisted = []  # List of Inactive Ticker Name String

        # yFinance Ouput
        self.yfinanceData = {
            "ticker_status": pd.DataFrame(),
            "historical_data": pd.DataFrame(),
            "financial_statements": pd.DataFrame(),
            "quarterly_financial_statements": pd.DataFrame(),
            "isin": pd.DataFrame(),
            "earnings_and_revenue": pd.DataFrame(),
            "quarterly_earnings_and_revenue": pd.DataFrame(),
            "majorHolders": pd.DataFrame(),
            "basic_shares": pd.DataFrame(),
            "stock_info": pd.DataFrame(),
            "stock_industry": pd.DataFrame(),
            "stock_calendar": pd.DataFrame(),
            "stock_recommendation": pd.DataFrame(),
            "stock_analysis": pd.DataFrame(),
            "stock_mfh": pd.DataFrame(),
            "stock_ih": pd.DataFrame()
        }

        # Check on Ticker Active/Inactive
        self.checkTickers()

    def checkTickers(self):
        activeTickers = []
        delisted = []

        # Check if ticker exist
        no_of_tickers = len(self.sgxTickers["ticker"])
        counter = 1
        for ticker in self.sgxTickers["ticker"]:
            print(
                f">> ========== Extracting {ticker} Overall Progress: ticker {counter}/{no_of_tickers}")
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

        return dfTickers

    def removeSI(self, ticker):
        return ticker[:-3]

    def getHistoricalData(self, start_date=dt.now()):
        # Listed Tickers' historical market data
        historical_data_df = pd.DataFrame()
        counter = 1
        for ticker in self.ticker_active:
            print(
                f">> ========== Extracting {ticker} Overall Progress: ticker {counter}/{len(self.ticker_active)}")
            if start_date is None:
                tickerHistoricalData = yf.download(
                    ticker.ticker, period='max', interval='1d', timeout=None)
                tickerHistoricalData["Tickers"] = self.removeSI(ticker.ticker)
                tickerHistoricalData["Market Status"] = "Market_Closed"

            else:
                start_date_string = start_date.strftime('%Y-%m-%d')
                tickerHistoricalData = yf.download(
                    ticker.ticker, start=start_date_string, interval='1d', timeout=None)
                tickerHistoricalData["Tickers"] = self.removeSI(ticker.ticker)

                if int(start_date.hour) < 12:
                    tickerHistoricalData["Market Status"] = "Market_Open"
                else:
                    tickerHistoricalData["Market Status"] = "Market_Closed"

            counter += 1

            historical_data_df = pd.concat(
                [historical_data_df, tickerHistoricalData])

        historical_data_df = historical_data_df.reset_index()

        # Store to Shared Data
        self.yfinanceData["historical_data"] = historical_data_df
        return historical_data_df

    def getFinancialStatement(self):
        financial_statements_df = pd.DataFrame()
        counter = 1
        for ticker in self.ticker_active:
            print(
                f">> ========== Overall Progress:{counter}/{len(self.ticker_active)} -  Extracting {ticker} ")
            profit_and_loss = ticker.financials.T.rename_axis(
                'Date').reset_index()

            balance_sheet = ticker.balance_sheet.T.rename_axis(
                'Date').reset_index()

            cashflow = ticker.cashflow.T.rename_axis(
                'Date').reset_index()

            financial_statements = reduce(lambda left_table, right_table: pd.merge(
                left_table, right_table, on='Date'), [profit_and_loss, balance_sheet, cashflow])
            financial_statements = financial_statements.fillna(np.NaN)
            # print(financial_statements)
            if type(financial_statements["Date"].values[0]) != str:
                financial_statements_df = pd.concat(
                    [financial_statements_df, financial_statements])
            counter += 1
            print(financial_statements_df)
        self.yfinanceData["financial_statements"] = financial_statements_df
        return financial_statements_df

    def getQuarterlyFinancialStatement(self):
        quarterly_financial_statements_df = pd.DataFrame()
        counter = 1
        for ticker in self.ticker_active:
            print(
                f">> ========== Overall Progress:{counter}/{len(self.ticker_active)} -  Extracting {ticker} ")
            profit_and_loss = ticker.quarterly_financials.T.rename_axis(
                'Date').reset_index()

            balance_sheet = ticker.quarterly_balancesheet.T.rename_axis(
                'Date').reset_index()

            cashflow = ticker.quarterly_cashflow.T.rename_axis(
                'Date').reset_index()

            financial_statements = reduce(lambda left_table, right_table: pd.merge(
                left_table, right_table, on='Date'), [profit_and_loss, balance_sheet, cashflow])
            financial_statements = financial_statements.fillna(np.NaN)
            # print(financial_statements)
            if type(financial_statements["Date"].values[0]) != str:
                quarterly_financial_statements_df = pd.concat(
                    [quarterly_financial_statements_df, financial_statements])
            counter += 1
            print(quarterly_financial_statements_df)
        self.yfinanceData["quarterly_financial_statements"] = quarterly_financial_statements_df
        return quarterly_financial_statements_df

    def getQuarterlyFinancialStatement_old(self):
        # Get quarterly financial statement (Financials, Balance Sheet, Cashflows)
        quarterly_financial_statements_df = pd.DataFrame()
        counter = 1

        for ticker in self.ticker_active:
            # get each quarter financial statement
            print(
                f">> ========== Current Progress: ticker {counter}/{len(self.ticker_active)}")

            pnl = ticker.quarterly_financials
            balance_sheet = ticker.quarterly_balancesheet
            cf = ticker.quarterly_cashflow

            # concatenate into one dataframe
            financial_statements = pd.concat(
                [pnl, balance_sheet, cf], axis=1).transpose()

            # Add ticker to dataframe
            financial_statements['Tickers'] = self.removeSI(ticker.ticker)
            quarterly_financial_statements_df = pd.concat(
                [quarterly_financial_statements_df, financial_statements])
            counter += 1
        quarterly_financial_statements_df = quarterly_financial_statements_df.reset_index()
        quarterly_financial_statements_df = quarterly_financial_statements_df.rename(
            columns={quarterly_financial_statements_df.columns[0]: 'Date'})

        quarterly_financial_statements_df["Date"] = quarterly_financial_statements_df["Date"].apply(
            lambda x: x if type(x) != str else np.NaN)
        quarterly_financial_statements_df = quarterly_financial_statements_df.dropna(subset=[
            "Date"])

        quarterly_financial_statements_df = quarterly_financial_statements_df.fillna(
            value=np.nan)

        # Store to Shared Data
        self.yfinanceData["quarterly_financial_statements"] = quarterly_financial_statements_df
        return quarterly_financial_statements_df

    def getISINcode(self):
        # Get ISIN code (International Securities Identification Number)
        isin_dict = {}
        for ticker in self.ticker_active:
            try:
                isin = ticker.isin
                isin_dict[self.removeSI(ticker.ticker)] = isin
            except:
                isin_dict[self.removeSI(ticker.ticker)] = np.nan

        isin_df = pd.DataFrame(list(isin_dict.items()), columns=[
                               'Tickers', 'ISIN'])

        # Store to Shared Data
        self.isin = isin_df
        return isin_df

    def getEarningsandRevenue(self):
        # Get Earnings and Revenue
        earnings_and_revenues_df = pd.DataFrame()

        for ticker in self.ticker_active:
            if (ticker.earnings.shape[0] < 1):
                # Ticker's revenue and earning do not exist
                ticker_earning_and_revenue = pd.DataFrame(
                    pd.Series({'Tickers': self.removeSI(ticker.ticker)})).transpose()
                earnings_and_revenues_df = pd.concat(
                    [earnings_and_revenues_df, ticker_earning_and_revenue])

            else:
                ticker_earning_and_revenue = ticker.earnings
                ticker_earning_and_revenue['Tickers'] = self.removeSI(
                    ticker.ticker)
                earnings_and_revenues_df = pd.concat(
                    [earnings_and_revenues_df, ticker_earning_and_revenue])

        earnings_and_revenues_df = earnings_and_revenues_df.reset_index().rename(columns={
            'index': 'Year'})

        # Store to Shared Data
        self.yfinanceData["earnings_and_revenue"] = earnings_and_revenues_df
        return earnings_and_revenues_df

    def getQuarterlyEarningsandRevenue(self):
        # Get Quarterly Earnings and Revenue
        quarterly_earnings_and_revenues_df = pd.DataFrame()

        for ticker in self.ticker_active:
            if (ticker.quarterly_earnings.shape[0] < 1):
                # Ticker's revenue and earning do not exist
                ticker_quarterly_earning_and_revenue = pd.DataFrame(
                    pd.Series({'Tickers': self.removeSI(ticker.ticker)})).transpose()
                quarterly_earnings_and_revenues_df = pd.concat(
                    [quarterly_earnings_and_revenues_df, ticker_quarterly_earning_and_revenue])

            else:
                ticker_quarterly_earning_and_revenue = ticker.quarterly_earnings
                ticker_quarterly_earning_and_revenue['Tickers'] = self.removeSI(
                    ticker.ticker)
                quarterly_earnings_and_revenues_df = pd.concat(
                    [quarterly_earnings_and_revenues_df, ticker_quarterly_earning_and_revenue])

        print(quarterly_earnings_and_revenues_df.columns)
        print(quarterly_earnings_and_revenues_df)

        quarterly_earnings_and_revenues_df = quarterly_earnings_and_revenues_df.reset_index(
        )

        first_col_name = list(quarterly_earnings_and_revenues_df.columns)[0]

        quarterly_earnings_and_revenues_df = quarterly_earnings_and_revenues_df.rename(
            columns={first_col_name: 'Quarters'})

        print(quarterly_earnings_and_revenues_df.columns)
        print(quarterly_earnings_and_revenues_df)

        quarterly_earnings_and_revenues_df['Quarters'] = quarterly_earnings_and_revenues_df['Quarters'].astype(
            str)

        # Store to Shared Data
        self.yfinanceData["quarterly_earnings_and_revenue"] = quarterly_earnings_and_revenues_df
        return quarterly_earnings_and_revenues_df

    def getMajorHolders(self):
        # Get Major Holders
        majorHolders_df = pd.DataFrame(columns=["Tickers", '% of Shares Held by All Insider', '% of Shares Held by Institutions',
                                       '% of Float Held by Institutions', 'Number of Institutions Holding Shares'])
        for ticker in self.ticker_active:
            if ticker.major_holders is None or ticker.major_holders.shape[0] != 4:
                ticker_majorHolders = pd.DataFrame(columns=["Tickers"])
                ticker_majorHolders["Tickers"] = self.removeSI(ticker.ticker)
                ticker_majorHolders["Tickers"] = ticker_majorHolders["Tickers"].astype(
                    str)
                majorHolders_df = majorHolders_df.merge(
                    ticker_majorHolders, how="outer", on=["Tickers"])
                print(majorHolders_df)
            else:
                # print(ticker.major_holders)
                ticker_majorHolders = ticker.major_holders.set_index(1)
                # print(ticker_majorHolders)
                ticker_majorHolders = ticker_majorHolders.T
                ticker_majorHolders["Tickers"] = self.removeSI(ticker.ticker)
                # print(ticker_majorHolders)
                majorHolders_df = majorHolders_df.merge(
                    ticker_majorHolders, how="outer", on=list(ticker_majorHolders.columns))
                print(majorHolders_df)
        self.yfinanceData["majorHolders"] = majorHolders_df
        return majorHolders_df

    def getBasicShares(self):
        # Get Basic Shares
        basic_shares_df = pd.DataFrame()
        for ticker in self.ticker_active:
            if ticker.shares is None:
                # Ticker does not have shares info
                ticker_share = pd.DataFrame(
                    pd.Series({'Tickers': self.removeSI(ticker.ticker)})).transpose()
                basic_shares_df = pd.concat(
                    [basic_shares_df, ticker_share])
            else:
                ticker_share = ticker.shares
                ticker_share['Tickers'] = self.removeSI(ticker.ticker)
                basic_shares_df = pd.concat(
                    [basic_shares_df, ticker_share])
        print(basic_shares_df)
        basic_shares_df = basic_shares_df.reset_index()
        # Store to Shared Data
        print(basic_shares_df)
        basic_shares_df = basic_shares_df.rename(
            columns={basic_shares_df.columns[0]: "Year"})
        self.yfinanceData["basic_shares"] = basic_shares_df
        print(basic_shares_df)
        return basic_shares_df

    def cast_dict_to_string(self, entry):
        if len(str(entry)) > 0 and str(entry)[0] in ["{", "["]:
            return str(entry)
        else:
            return entry

    def getStockInfo(self):
        # Get stock information
        all_tickers_dict = {}
        for ticker in self.ticker_active:
            if ticker.info is None:
                all_tickers_dict[self.removeSI(ticker.ticker)] = pd.Series()
            else:
                print(ticker.info)
                ticker_info = pd.Series(ticker.info)
                print(ticker_info)
                all_tickers_dict[self.removeSI(
                    ticker.ticker)] = pd.Series(ticker_info)
        all_tickers_info = pd.DataFrame(all_tickers_dict).transpose(
        )

        all_tickers_info = all_tickers_info.reset_index()

        all_tickers_info = all_tickers_info.rename(
            columns={'index': 'Tickers'})

        for column in all_tickers_info.columns:
            # all_tickers_info[column] = all_tickers_info[column].apply(lambda x: str(x) if ((str(x)+" ")[
            #     0] in ["{", "["]) else x)
            all_tickers_info[column] = all_tickers_info[column].apply(
                self.cast_dict_to_string)

        print(all_tickers_info)
        print(all_tickers_info.columns)
        for c in all_tickers_info.columns:
            print(c)
        print("DTYPES HERE - all ticker info")
        print(all_tickers_info.dtypes)
        all_tickers_info.to_csv("all_ticker.csv")
        self.yfinanceData["stock_info"] = all_tickers_info

        return all_tickers_info

    def getStockIndustry(self):
        if self.yfinanceData["stock_info"].empty:
            self.getStockInfo()
        stock_info = self.yfinanceData["stock_info"]
        stock_industry = stock_info[["Tickers", "industry"]]
        stock_industry = stock_industry.rename(columns={"Tickers": "ticker"})

        # Store to Shared Data
        self.yfinanceData["stock_industry"] = stock_industry
        return stock_industry

    def getCalendar(self):
        # Get next event (earnings, etc)
        stock_calendar_df = pd.DataFrame()
        for ticker in self.ticker_active:
            if ticker.calendar is None:
                ticker_calendar = pd.DataFrame(
                    pd.Series({'Tickers': self.removeSI(ticker.ticker)})).transpose()
                stock_calendar_df = pd.concat(
                    [stock_calendar_df, ticker_calendar])

            else:
                ticker_calendar = ticker.calendar.transpose()
                ticker_calendar['Ticker'] = self.removeSI(ticker.ticker)
                stock_calendar_df = pd.concat(
                    [stock_calendar_df, ticker_calendar])
        print(stock_calendar_df)
        stock_calendar_df = stock_calendar_df.reset_index(drop=True)
        # Store to Shared Data
        print(stock_calendar_df)
        for c in stock_calendar_df.columns:
            print(c)

        self.yfinanceData["stock_calendar"] = stock_calendar_df
        return stock_calendar_df

    def getRecommendations(self):
        # Get Recommendations
        recommendations_df = pd.DataFrame()

        for ticker in self.ticker_active:
            if ticker.recommendations is None:
                ticker_recommendations = pd.DataFrame(
                    pd.Series({'Tickers': self.removeSI(ticker.ticker)})).transpose()
                recommendations_df = pd.concat(
                    [recommendations_df, ticker_recommendations])

            else:
                ticker_recommendations = ticker.recommendations
                ticker_recommendations['Tickers'] = self.removeSI(
                    ticker.ticker)
                recommendations_df = pd.concat(
                    [recommendations_df, ticker_recommendations])
        recommendations_df = recommendations_df.reset_index()

        # Store to Shared Data
        self.yfinanceData["stock_recommendation"] = recommendations_df
        return recommendations_df

    def getAnalysis(self):
        # Get Analysis
        analysis_df = pd.DataFrame()
        for ticker in self.ticker_active:
            if ticker.analysis is None:
                ticker_analysis = pd.DataFrame(
                    pd.Series({'Tickers': self.removeSI(ticker.ticker)})).transpose()
                analysis_df = pd.concat(
                    [analysis_df, ticker_analysis])

            else:
                ticker_analysis = ticker.analysis
                ticker_analysis['Tickers'] = self.removeSI(ticker.ticker)
                analysis_df = pd.concat(
                    [analysis_df, ticker_analysis])

        analysis_df = analysis_df.reset_index().rename(columns={
            'index': 'Period'})
        analysis_df['Period'] = analysis_df['Period'].astype(str)

        # Store to Shared Data
        self.yfinanceData["stock_analysis"] = analysis_df
        return analysis_df

    def getMutualFundHolders(self):
        # Get Mutual Fund Holders
        mfh_pd = pd.DataFrame()

        for ticker in self.ticker_active:
            if ticker.mutualfund_holders is None:
                ticker_mfh = pd.DataFrame(
                    pd.Series({'Tickers': self.removeSI(ticker.ticker)})).transpose()
                mfh_pd = pd.concat([mfh_pd, ticker_mfh])

            else:
                ticker_mfh = ticker.mutualfund_holders
                ticker_mfh['Tickers'] = self.removeSI(ticker.ticker)
                mfh_pd = pd.concat([mfh_pd, ticker_mfh])

        mfh_pd = mfh_pd.reset_index(drop=True)

        # Store to Shared Data
        self.yfinanceData["stock_mfh"] = mfh_pd
        return mfh_pd

    def getInstitutionalHolders(self):
        # Get Institutional holders
        ih_pd = pd.DataFrame()

        for ticker in self.ticker_active:
            if ticker.institutional_holders is None or ticker.institutional_holders.shape[1] != 5:
                ticker_ih = pd.DataFrame(pd.DataFrame(
                    pd.Series({'Tickers': self.removeSI(ticker.ticker)})).transpose())
                ih_pd = pd.concat([ih_pd, ticker_ih])

            else:
                ticker_ih = ticker.institutional_holders
                ticker_ih['Tickers'] = self.removeSI(ticker.ticker)
                ih_pd = pd.concat([ih_pd, ticker_ih])

        ih_pd = ih_pd.reset_index(drop=True)

        # Store to Shared Data
        self.yfinanceData["stock_ih"] = ih_pd

        return ih_pd

    def yfinanceQuery(self):

        print("Query Historical Data")
        self.getHistoricalData()
        print("Historical Data Query Complete")

        print("Query Financial Statement")
        self.getFinancialStatement()
        print("Financial Statement Query Complete")

        print(">> ========== START: Quarterly Financial Statement Query")
        print(self.getQuarterlyFinancialStatement())
        print(">> ========== COMPLETE: Quarterly Financial Statement Query")

        print(">> ========== START: ISIN Code Query")
        self.getISINcode()
        print(">> ========== COMPLETE: ISIN Code Query")

        print("Query Earnings and Revenue")
        self.getEarningsandRevenue()
        print("Earnings and Revenue Query Complete")

        print("Query Quarterly Earnings and Revenue")
        self.getQuarterlyEarningsandRevenue()
        print("Quarterly Earnings and Revenue Query Complete")

        print(">> ========== START: Major Holders Query")
        self.getMajorHolders()
        print(">> ========== COMPLETE: Major Holders Query")

        print(">> ========== START: Basic Shares Query")
        self.getBasicShares()
        print(">> ========== COMPLETE: Basic Shares Query")

        print("Query Stock Info")
        self.getStockInfo()
        print("Stock Info Query Complete")

        print(">> ========== START: Stock Industry Extraction")
        self.getStockIndustry()
        print(">> ========== COMPLETE: Stock Industry Extraction")

        print(">> ========== START: Stock Calendar Query")
        self.getCalendar()
        print(">> ========== COMPLETE: Stock Calendar Query")

        print(">> ========== START: Analyst Recommendations Query")
        self.getRecommendations()
        print(">> ========== COMPLETE: Analyst Recommendations Query")

        print(">> ========== START: Stock Analysis Query")
        self.getAnalysis()
        print(">> ========== COMPLETE: Stock Analysis Query")

        print(">> ========== START: Mutual Fund Holders Query")
        self.getMutualFundHolders()
        print(">> ========== COMPLETE: Mutual Fund Holders Query")

        print(">> ========== START: Institutional Holders Querys")
        self.getInstitutionalHolders()
        print(">> ========== COMPLETE: Institutional Holders Query")

        for name, df in self.yfinanceData.items():
            df.to_csv(f"output_store/{name}.csv", index=False)

        return self.yfinanceData
