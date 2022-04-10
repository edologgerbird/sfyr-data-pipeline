from datetime import datetime as dt, date as date
import pandas as pd
import numpy as np
import yfinance as yf


class yFinanceExtractor:
    def __init__(self, sgxTickers):
        self.sgxTickers = sgxTickers
        self.sgxTickers.company_code = self.sgxTickers.company_code.str[:] + ".SI"
        self.ticker_active = []
        self.ticker_delisted = []
        self.ticker_with_status = pd.DataFrame()
        self.all_ticker_info = pd.DataFrame()
        self.checkTickers()

    def checkTickers(self):
        activeTickers = []
        delisted = []

        # Check if ticker exist
        for ticker in self.sgxTickers["company_code"]:
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

    def getHistoricalData(self, start_date=dt.now()):
        # Listed Tickers' historical market data
        historicalData = pd.DataFrame()
        for ticker in self.ticker_active:
            if start_date is None:
                tickerHistoricalData = yf.download(
                    ticker, period='max', interval='1d', timeout=None)
                tickerHistoricalData["Tickers"] = ticker
                tickerHistoricalData["Market Status"] = "Closed"

            else:
                start_date_string = start_date.strftime('%Y-%m-%d')
                tickerHistoricalData = yf.download(
                    ticker, start=start_date_string, interval='1d', timeout=None)
                tickerHistoricalData["Tickers"] = ticker

                if start_date.time() < date.today().replace(hour=10, minute=10):
                    tickerHistoricalData["Market Status"] = "Open"
                else:
                    tickerHistoricalData["Market Status"] = "Closed"

            historicalData = pd.concat([historicalData, tickerHistoricalData])

        historicalData = historicalData.reset_index()

        # Generate all existing tickers' history
        return historicalData

    def getFinancialStatement(self):
        # Retrieve financial statement (Financials, Balance Sheet and Cash flow)
        financial_statements_dataframe = pd.DataFrame()

        for ticker in self.ticker_active:
            # get each financial statement
            pnl = ticker.financials
            balance_sheet = ticker.balancesheet
            cf = ticker.cashflow

            # concatenate into one dataframe
            financial_statements = pd.concat(
                [pnl, balance_sheet, cf], axis=1).transpose()

            # Add ticker to dataframe
            financial_statements['Tickers'] = ticker.ticker
            financial_statements_dataframe = pd.concat(
                [financial_statements_dataframe, financial_statements])
        financial_statements_dataframe = financial_statements_dataframe.reset_index(
        ).rename(columns={financial_statements_dataframe.index.name: 'Date'})
        return financial_statements_dataframe

    def getQuarterlyFinancialStatement(self):
        # Get quarterly financial statement (Financials, Balance Sheet, Cashflows)
        financial_statements_dataframe = pd.DataFrame()

        for ticker in self.ticker_active:
            # get each quarter financial statement
            pnl = ticker.quarterly_financials
            balance_sheet = ticker.quarterly_balancesheet
            cf = ticker.quarterly_cashflow

            # concatenate into one dataframe
            financial_statements = pd.concat(
                [pnl, balance_sheet, cf], axis=1).transpose()

            # Add ticker to dataframe
            financial_statements['Tickers'] = ticker.ticker
            financial_statements_dataframe = pd.concat(
                [financial_statements_dataframe, financial_statements])
        financial_statements_dataframe = financial_statements_dataframe.reset_index(
        ).rename(columns={financial_statements_dataframe.index.name: 'Date'})
        return financial_statements_dataframe

    def getISINcode(self):
        # Get ISIN code (International Securities Identification Number)
        isin_dict = {}
        for ticker in self.ticker_active:
            try:
                isin = ticker.isin
                isin_dict[ticker.ticker] = isin
            except:
                isin_dict[ticker.ticker] = np.nan

        df_isin = pd.DataFrame(list(isin_dict.items()), columns=[
                               'Tickers', 'ISIN'])
        return df_isin

    def getEarningsandRevenue(self):
        # Get Earnings and Revenue
        all_tickers_earnings_and_revenues = pd.DataFrame()

        for ticker in self.ticker_active:
            if (ticker.earnings.shape[0] < 1):
                # Ticker's revenue and earning do not exist
                data = {'Revenue': np.nan, 'Earnings': np.nan,
                        'Tickers': ticker.ticker}
                ticker_earning_and_revenue = pd.DataFrame(data, index=[np.nan])
                all_tickers_earnings_and_revenues = pd.concat(
                    [all_tickers_earnings_and_revenues, ticker_earning_and_revenue])

            else:
                ticker_earning_and_revenue = ticker.earnings
                ticker_earning_and_revenue['Tickers'] = ticker.ticker
                all_tickers_earnings_and_revenues = pd.concat(
                    [all_tickers_earnings_and_revenues, ticker_earning_and_revenue])
        return all_tickers_earnings_and_revenues

    def getQuarterlyEarningsandRevenue(self):
        # Get Quarterly Earnings and Revenue
        all_tickers_quarterly_earnings_and_revenues = pd.DataFrame()

        for ticker in self.ticker_active:
            if (ticker.quarterly_earnings.shape[0] < 1):
                # Ticker's revenue and earning do not exist
                data = {'Revenue': np.nan, 'Earnings': np.nan,
                        'Tickers': ticker.ticker}
                ticker_quarterly_earning_and_revenue = pd.DataFrame(data, index=[
                                                                    np.nan])
                all_tickers_quarterly_earnings_and_revenues = pd.concat(
                    [all_tickers_quarterly_earnings_and_revenues, ticker_quarterly_earning_and_revenue])

            else:
                ticker_quarterly_earning_and_revenue = ticker.quarterly_earnings
                ticker_quarterly_earning_and_revenue['Tickers'] = ticker.ticker
                all_tickers_quarterly_earnings_and_revenues = pd.concat(
                    [all_tickers_quarterly_earnings_and_revenues, ticker_quarterly_earning_and_revenue])

        all_tickers_quarterly_earnings_and_revenues = all_tickers_quarterly_earnings_and_revenues.reset_index()
        return all_tickers_quarterly_earnings_and_revenues

    def getMajorHolders(self):
        # Get Major Holders
        all_tickers_majorHolders = pd.DataFrame()
        for ticker in self.ticker_active:
            if ticker.major_holders is None or ticker.major_holders.shape[0] != 4:
                data = {'% of Shares Held by All Insider': np.nan, '% of Shares Held by Institutions': np.nan,
                        '% of Float Held by Institutions': np.nan, 'Number of Institutions Holding Shares': np.nan,
                        'Tickers': ticker.ticker}
                ticker_majorHolders = pd.DataFrame(data, index=[0])
                all_tickers_majorHolders = pd.concat(
                    [all_tickers_majorHolders, ticker_majorHolders])

            else:
                ticker_majorHolders = ticker.major_holders[0].rename(
                    {0: '% of Shares Held by All Insider', 1: '% of Shares Held by Institutions', 2: '% of Float Held by Institutions', 3: 'Number of Institutions Holding Shares'})
                ticker_majorHolders['Tickers'] = ticker.ticker
                all_tickers_majorHolders = all_tickers_majorHolders.append(
                    ticker_majorHolders)
        all_tickers_majorHolders = all_tickers_majorHolders.reset_index(
            drop=True)
        return all_tickers_majorHolders

    def getBasicShares(self):
        # Get Basic Shares
        all_tickers_shares = pd.DataFrame()
        for ticker in self.ticker_active:
            if ticker.shares is None:
                # Ticker does not have shares info
                data = {'BasicShares': np.nan, 'Tickers': ticker.ticker}
                ticker_share = pd.DataFrame(data, index=[np.nan])
                all_tickers_shares = pd.concat(
                    [all_tickers_shares, ticker_share])
            else:
                ticker_share = ticker.shares
                ticker_share['Tickers'] = ticker.ticker
                all_tickers_shares = pd.concat(
                    [all_tickers_shares, ticker_share])
        all_tickers_shares = all_tickers_shares.reset_index()
        return all_tickers_shares

    def getStockInfo(self):
        # Get stock information
        all_tickers_dict = {}
        for ticker in self.ticker_active:
            if ticker.info is None:
                all_tickers_dict[ticker.ticker] = pd.Series()
            else:
                all_tickers_dict[ticker.ticker] = pd.Series(ticker.info)
        all_tickers_info = pd.DataFrame(all_tickers_dict).transpose(
        ).reset_index().rename(columns={'index': 'Tickers'})
        self.all_ticker_info = all_tickers_info
        return all_tickers_info

    def getStockIndustry(self):
        if self.all_ticker_info.empty:
            self.getStockInfo()
        stockIndusty = self.all_ticker_info[["Tickers", "industry"]]
        return stockIndusty

    def getCalendar(self):
        # Get next event (earnings, etc)
        all_calendar = pd.DataFrame()
        for ticker in self.ticker_active:
            if ticker.calendar is None:
                data = {'Earnings Date': np.nan, 'Earnings Average': np.nan, 'Earnings Low': np.nan, 'Earnings High': np.nan, 'Revenue Average': np.nan,
                        'Revenue Low': np.nan,    'Revenue High': np.nan,    'Tickers': ticker.ticker}
                ticker_calendar = pd.DataFrame(data, index=['Value'])
                all_calendar = pd.concat([all_calendar, ticker_calendar])

            else:
                ticker_calendar = ticker.calendar.transpose()
                ticker_calendar['Ticker'] = ticker.ticker
                all_calendar = pd.concat([all_calendar, ticker_calendar])
            return all_calendar

    def getRecommendations(self):
        # Get Recommendations
        all_tickers_recommendations = pd.DataFrame()

        for ticker in self.ticker_active:
            if ticker.recommendations is None:
                data = {'Firm': np.nan, 'To Grade': np.nan, 'From Grade': np.nan,
                        'Action': np.nan, "Tickers": ticker.ticker}
                ticker_recommendations = pd.DataFrame(data, index=[np.nan])
                all_tickers_recommendations = pd.concat(
                    [all_tickers_recommendations, ticker_recommendations])

            else:
                ticker_recommendations = ticker.recommendations
                ticker_recommendations['Tickers'] = ticker.ticker
                all_tickers_recommendations = pd.concat(
                    [all_tickers_recommendations, ticker_recommendations])
        all_tickers_recommendations = all_tickers_recommendations.reset_index()
        return all_tickers_recommendations

    def getAnalysis(self):
        # Get Analysis
        all_tickers_analysis = pd.DataFrame()
        for ticker in self.ticker_active:
            if ticker.analysis is None:
                data = {'Max Age': np.nan, 'End Date': np.nan, 'Growth': np.nan, 'Earnings Estimate Avg': np.nan,
                        'Earnings Estimate Low': np.nan, 'Earnings Estimate High': np.nan,
                        'Earnings Estimate Year Ago Eps': np.nan,
                        'Earnings Estimate Number Of Analysts': np.nan, 'Earnings Estimate Growth': np.nan,
                        'Revenue Estimate Avg': np.nan, 'Revenue Estimate Low': np.nan, 'Revenue Estimate High': np.nan,
                        'Revenue Estimate Number Of Analysts': np.nan,
                        'Revenue Estimate Year Ago Revenue': np.nan, 'Revenue Estimate Growth': np.nan,
                        'Eps Trend Current': np.nan, 'Eps Trend 7Days Ago': np.nan, 'Eps Trend 30Days Ago': np.nan,
                        'Eps Trend 60Days Ago': np.nan, 'Eps Trend 90Days Ago': np.nan,
                        'Eps Revisions Up Last7Days': np.nan, 'Eps Revisions Up Last30Days': np.nan,
                        'Eps Revisions Down Last30Days': np.nan, 'Eps Revisions Down Last90Days': np.nan, 'Tickers': ticker.ticker}
                ticker_analysis = pd.DataFrame(data, index=[np.nan])
                all_tickers_analysis = pd.concat(
                    [all_tickers_analysis, ticker_analysis])

            else:
                ticker_analysis = ticker.analysis
                ticker_analysis['Tickers'] = ticker.ticker
                all_tickers_analysis = pd.concat(
                    [all_tickers_analysis, ticker_analysis])

        all_tickers_analysis = all_tickers_analysis.reset_index().rename(columns={
            'index': 'Period'})
        return all_tickers_analysis

    def getMutualFundHolders(self):
        # Get Mutual Fund Holders
        all_tickers_mfh = pd.DataFrame()

        for ticker in self.ticker_active:
            if ticker.mutualfund_holders is None:
                data = {'Holder': np.nan, 'Shares': np.nan, 'Date Reported': np.nan,
                        '% Out': np.nan, 'Value': np.nan, 'Tickers': ticker.ticker}
                ticker_mfh = pd.DataFrame(data, index=[0])
                all_tickers_mfh = pd.concat([all_tickers_mfh, ticker_mfh])

            else:
                ticker_mfh = ticker.mutualfund_holders
                ticker_mfh['Tickers'] = ticker.ticker
                all_tickers_mfh = pd.concat([all_tickers_mfh, ticker_mfh])

        all_tickers_mfh = all_tickers_mfh.reset_index(drop=True)
        return all_tickers_mfh

    def getInstitutionalHolders(self):
        # Get Institutional holders
        all_tickers_ih = pd.DataFrame()

        for ticker in self.ticker_active:
            if ticker.institutional_holders is None or ticker.institutional_holders.shape[1] != 5:
                data = {'Holder': np.nan, 'Shares': np.nan, 'Date Reported': np.nan,
                        '% Out': np.nan, 'Value': np.nan, 'Tickers': ticker.ticker}
                ticker_ih = pd.DataFrame(data, index=[0])
                all_tickers_ih = pd.concat([all_tickers_ih, ticker_ih])

            else:
                ticker_ih = ticker.institutional_holders
                ticker_ih['Tickers'] = ticker.ticker
                all_tickers_ih = pd.concat([all_tickers_ih, ticker_ih])

        all_tickers_ih = all_tickers_ih.reset_index(drop=True)
        return all_tickers_ih

    # def getDictionary(self):
    #     dict = {}

    #     # Getting listed and delisted tickers
    #     dict['Ticker Listing'] = self.checkTickers()

    #     # Getting listed tickers history data
    #     dict['Historical Data'] = self.getHistoricalData(listed)

    #     tickers = [yf.Ticker(ticker) for ticker in listed]
    #     # Getting Financial Statement
    #     dict['Financial Statement'] = self.getFinancialStatement()

    #     # Getting Quarterly Financial Statement
    #     dict['Quarterly Financial Statement'] = self.getQuarterlyFinancialStatement()

    #     # Getting ISIN code (International Securities Identification Number)
    #     dict['isin'] = self.getISINcode()

    #     # Getting tickers revenues and earnings
    #     dict['Revenues and Earnings'] = self.getEarningsandRevenue()

    #     # Getting tickers quarterly revenues and earnings
    #     dict['Quarterly Revenues and Earnings'] = self.getQuarterlyEarningsandRevenue()

    #     # Getting tickers major holders
    #     dict['Major Holders'] = self.getMajorHolders()

    #     # Getting tickers basic shares
    #     dict['Basic Shares'] = self.getBasicShares()

    #     # Getting tickers stock information
    #     dict['Stock Information'] = self.getStockInfo()

    #     # Getting tickers sustainability
    #     dict['sustainability'] = self.getSustainability()

    #     # Getting tickers calendar - next events
    #     dict['calendar'] = self.getCalendar()

    #     # Getting tickers recommendations
    #     dict['recommendations'] = self.getRecommendations()

    #     # Getting tickers analysis
    #     dict['analysis'] = self.getAnalysis()

    #     # Getting tickers Mutual Fund Holders
    #     dict['Mutal Fund Holders'] = self.getMutualFundHolders()

    #     # Getting tickers Institutional Holders
    #     dict['Institutional Holders'] = self.getInstitutionalHolders()
    #     return dict
