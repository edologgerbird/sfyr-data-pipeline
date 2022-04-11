from datetime import datetime as dt
import pandas as pd
import numpy as np
import yfinance as yf


class yFinanceExtractor:
    def __init__(self, sgxTickers):
        self.sgxTickers = sgxTickers
        self.sgxTickers.ticker = self.sgxTickers.ticker.str[:] + ".SI"

        # Initalisation of Shared Data
        self.ticker_active = []  # List of Active Ticker Objects
        self.ticker_delisted = []  # List of Inactive Ticker Name String
        self.ticker_with_status = pd.DataFrame()
        self.historical_data = pd.DataFrame()
        self.financial_statements = pd.DataFrame()
        self.quarterly_financial_statements = pd.DataFrame()
        self.isin = pd.DataFrame()
        self.earnings_and_revenue = pd.DataFrame()
        self.quarterly_earnings_and_revenue = pd.DataFrame()
        self.majorHolders = pd.DataFrame()
        self.basic_shares = pd.DataFrame()
        self.stock_info = pd.DataFrame()
        self.stock_industry = pd.DataFrame()
        self.stock_calendar = pd.DataFrame()
        self.stock_recommendation = pd.DataFrame()
        self.stock_analysis = pd.DataFrame()
        self.stock_mfh = pd.DataFrame()
        self.stock_ih = pd.DataFrame()

        # Check on Ticker Active/Inactive
        self.checkTickers()

    def checkTickers(self):
        activeTickers = []
        delisted = []

        # Check if ticker exist
        for ticker in self.sgxTickers["ticker"]:
            length = yf.download(
                ticker, period='max', interval='1d', timeout=None).shape[0]
            if length > 0:
                activeTickers.append(ticker)
            else:
                delisted.append(ticker)

        # Generate dataframe of existing and missing tickers
        dfTickers = pd.DataFrame({'Listed': activeTickers})
        dfTickers["Delisted"] = pd.Series(delisted)

        # Update Class Copy
        self.ticker_with_status = dfTickers
        self.ticker_active = [yf.Ticker(ticker) for ticker in activeTickers]
        self.ticker_delisted = delisted

        return dfTickers

    def removeSI(self, ticker):
        return ticker[:-3]

    def getHistoricalData(self, start_date=dt.now()):
        # Listed Tickers' historical market data
        historical_data_df = pd.DataFrame()
        for ticker in self.ticker_active:
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

            historical_data_df = pd.concat(
                [historical_data_df, tickerHistoricalData])

        historical_data_df = historical_data_df.reset_index()

        # Store to Shared Data
        self.historical_data = historical_data_df
        return historical_data_df

    def getFinancialStatement(self):
        # Retrieve financial statement (Financials, Balance Sheet and Cash flow)
        financial_statements_df = pd.DataFrame()

        for ticker in self.ticker_active:
            # get each financial statement
            pnl = ticker.financials
            balance_sheet = ticker.balancesheet
            cf = ticker.cashflow

            # concatenate into one dataframe
            financial_statements = pd.concat(
                [pnl, balance_sheet, cf], axis=1).transpose()

            # Add ticker to dataframe
            financial_statements['Tickers'] = self.removeSI(ticker.ticker)
            financial_statements_df = pd.concat(
                [financial_statements_df, financial_statements])
        financial_statements_df = financial_statements_df.reset_index(
        ).rename(columns={financial_statements_df.index.name: 'Date'})

        # Store to Shared Data
        self.financial_statements = financial_statements_df
        return financial_statements_df

    def getQuarterlyFinancialStatement(self):
        # Get quarterly financial statement (Financials, Balance Sheet, Cashflows)
        quarterly_financial_statements_df = pd.DataFrame()

        for ticker in self.ticker_active:
            # get each quarter financial statement
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
        quarterly_financial_statements_df = quarterly_financial_statements_df.reset_index(
        ).rename(columns={quarterly_financial_statements_df.index.name: 'Date'})

        # Store to Shared Data
        self.quarterly_financial_statements = quarterly_financial_statements_df
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
        self.earnings_and_revenue = earnings_and_revenues_df
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

        quarterly_earnings_and_revenues_df = quarterly_earnings_and_revenues_df.reset_index(
        ).rename(columns={'index': 'Quarters'})

        # Store to Shared Data
        self.quarterly_earnings_and_revenue = quarterly_earnings_and_revenues_df
        return quarterly_earnings_and_revenues_df

    def getMajorHolders(self):
        # Get Major Holders
        majorHolders_df = pd.DataFrame()
        for ticker in self.ticker_active:
            if ticker.major_holders is None or ticker.major_holders.shape[0] != 4:
                ticker_majorHolders = pd.DataFrame(
                    pd.Series({'Tickers': self.removeSI(ticker.ticker)}))
                majorHolders_df = pd.concat(
                    [majorHolders_df, ticker_majorHolders])

            else:
                ticker_majorHolders = ticker.major_holders[0].rename(
                    {0: '% of Shares Held by All Insider', 1: '% of Shares Held by Institutions', 2: '% of Float Held by Institutions', 3: 'Number of Institutions Holding Shares'})
                ticker_majorHolders['Tickers'] = self.removeSI(ticker.ticker)
                ticker_majorHolders = ticker_majorHolders.transpose()
                majorHolders_df = pd.concat(
                    [majorHolders_df, ticker_majorHolders], axis=1)
        majorHolders_df = majorHolders_df.transpose().reset_index(
            drop=True)

        # Store to Shared Data
        self.majorHolders = majorHolders_df
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
        basic_shares_df = basic_shares_df.reset_index()
        # Store to Shared Data
        self.basic_shares = basic_shares_df
        return basic_shares_df

    def getStockInfo(self):
        # Get stock information
        all_tickers_dict = {}
        for ticker in self.ticker_active:
            if ticker.info is None:
                all_tickers_dict[self.removeSI(ticker.ticker)] = pd.Series()
            else:
                all_tickers_dict[self.removeSI(
                    ticker.ticker)] = pd.Series(ticker.info)
        all_tickers_info = pd.DataFrame(all_tickers_dict).transpose(
        ).reset_index().rename(columns={'index': 'Tickers'})
        self.stock_info = all_tickers_info
        return all_tickers_info

    def getStockIndustry(self):
        if self.stock_info.empty:
            self.getStockInfo()
        stock_industry = self.stock_info[["Tickers", "industry"]]

        # Store to Shared Data
        self.stock_industry = stock_industry
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
        stock_calendar_df = stock_calendar_df.reset_index(drop=True)
        # Store to Shared Data
        self.stock_calendar = stock_calendar_df
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
        self.stock_recommendation = recommendations_df
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

        # Store to Shared Data
        self.stock_analysis = analysis_df
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
        self.stock_mfh = mfh_pd
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
        self.stock_ih = ih_pd

        return ih_pd