from datetime import datetime as dt
from datetime import timedelta
from datetime import date
import pandas as pd
import numpy as np
import yfinance as yf
from functools import reduce
from data_load.bigQueryAPI import bigQueryDB


class yFinanceExtractor:
    def __init__(self):
        self.datasetTable = "SGX.Tickers"
        self.gbqQueryTickers = bigQueryDB().getDataFields(
            self.datasetTable, "company_code")
        self.gbqQueryTickers.company_code = self.gbqQueryTickers.company_code.str[:] + ".SI"
        self.ticker_active = []
        self.ticker_delisted = []
        self.ticker_with_status = pd.DataFrame()

    def checkTickers(self):
        activeTickers = []
        delisted = []

        # Check if ticker exist
        for t in self.gbqQueryTickers["company_code"]:
            length = yf.download(
                t, period='max', interval='1d', timeout=None).shape[0]
            if length > 0:
                activeTickers.append(t)
            else:
                delisted.append(t)

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
            financial_statements = pd.concat([pnl, bs, cf], axis=1).transpose()

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
            financial_statements = pd.concat([pnl, bs, cf], axis=1).transpose()

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
                               'Tickers', 'International Securities Identification Number'])
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

    # NEED TO REFORMAT THE INDEXING
    def getStockInfo(self):
        # Get stock information
        all_info = pd.DataFrame()

        for ticker in self.ticker_active:
            if ticker.info is None:
                all_info[ticker.ticker] = np.nan
            else:
                stockInfo = pd.DataFrame(ticker.info.values(
                ), index=ticker.info.keys(), columns=[ticker.ticker])
                all_info = pd.concat([all_info, stockInfo])
        return all_info

    def getOptionsExpirations(self):
        # Get options expirations
        options_dict = {}

        for ticker in self.ticker_active:
            options_dict[ticker.ticker] = list(ticker.options)
            df_options = pd.DataFrame.from_dict(
                options_dict, orient='index').transpose()
        return df_options

    def getSustainability(self):
        # Get Sustainability
        all_sustainability = pd.DataFrame()

        for ticker in self.ticker_active:
            if ticker.sustainability is None:
                all_sustainability[ticker.ticker] = np.nan
            else:
                sustainability = ticker.sustainability.rename(
                    columns={"Value": ticker.ticker})
                all_sustainability = pd.concat(
                    [all_sustainability, sustainability])
        return all_sustainability

    def getCalendar(self):
        # Get next event (earnings, etc)
        all_calendar = pd.DataFrame()
        for ticker in self.ticker_active:
            if ticker.calendar is None:
                data = {'Earnings Date': np.nan, 'Earnings Average': np.nan, 'Earnings Low': np.nan, 'Earnings High': np.nan, 'Revenue Average': np.nan,
                        'Revenue Low': np.nan,    'Revenue High': np.nan,    'Tickers': ticker.ticker}
                ticker_calendar = pd.DataFrame(data, index=['Value'])
                calendar = pd.concat([all_calendar, ticker_calendar])

            else:
                ticker_calendar = ticker.calendar.transpose()
                ticker_calendar['Ticker'] = ticker.ticker
                all_calendar = pd.concat([all_calendar, ticker_calendar])
            return all_calendar

    def getRecommendations(self):
        # Get Recommendations
        dfs = []  # list for each ticker's dataframe
        re = pd.DataFrame()

        for ticker in self.ticker_active:
            if ticker.recommendations is None:
                pass

            else:
                # get each recommendations
                re = ticker.recommendations

                # concatenate into one dataframe
                re = pd.concat([re])

                # make dataframe format nicer
                # Swap dates and columns
                data = re.T
                # reset index (date) into a column
                data = data.reset_index()
                # Rename old index from '' to Date
                data.columns = ['Date', *data.columns[1:]]
                # Add ticker to dataframe
                data['Tickers'] = ticker.ticker
                dfs.append(data)

        parser = pd.io.parsers.base_parser.ParserBase({'usecols': None})

        for reco in dfs:
            reco.columns = parser._maybe_dedup_names(reco.columns)

        if len(dfs) > 0:
            reco = pd.concat(dfs, ignore_index=True)
            reco = reco.set_index(['Tickers', 'Date'])
        else:
            reco = pd.DataFrame(
                columns=['Firm', 'To Grade', 'From Grade', 'Action'])
        return reco

    def getAnalysis(self):
        # Get Analysis
        dfs = []  # list for each ticker's dataframe
        an = pd.DataFrame()

        for ticker in self.ticker_active:
            if ticker.analysis is None:
                pass

            else:
                # get each analysis
                analysis = ticker.analysis

                # concatenate into one dataframe
                an = pd.concat([analysis])

                # make dataframe format nicer
                # Swap dates and columns
                data = an.T
                # reset index (date) into a column
                data = data.reset_index()
                # Rename old index from '' to Date
                data.columns = ['Date', *data.columns[1:]]
                # Add ticker to dataframe
                data['Tickers'] = ticker.ticker
                dfs.append(data)

        parser = pd.io.parsers.base_parser.ParserBase({'usecols': None})

        for analysis in dfs:
            analysis.columns = parser._maybe_dedup_names(analysis.columns)

        if len(dfs) > 0:
            analysis = pd.concat(dfs, ignore_index=True)
            analysis = analysis.set_index(['Tickers', 'Date'])
        else:
            analysis = pd.DataFrame(columns=['Max Age', 'End Date', 'Growth', 'Earnings Estimate Avg',
                                             'Earnings Estimate Low', 'Earnings Estimate High',
                                             'Earnings Estimate Year Ago Eps',
                                             'Earnings Estimate Number Of Analysts', 'Earnings Estimate Growth',
                                             'Revenue Estimate Avg', 'Revenue Estimate Low', 'Revenue Estimate High',
                                             'Revenue Estimate Number Of Analysts',
                                             'Revenue Estimate Year Ago Revenue', 'Revenue Estimate Growth',
                                             'Eps Trend Current', 'Eps Trend 7Days Ago', 'Eps Trend 30Days Ago',
                                             'Eps Trend 60Days Ago', 'Eps Trend 90Days Ago',
                                             'Eps Revisions Up Last7Days', 'Eps Revisions Up Last30Days',
                                             'Eps Revisions Down Last30Days', 'Eps Revisions Down Last90Days'])
        return analysis

    def getMutualFundHolders(self):
        # Get Mutual Fund Holders
        dfs = []  # list for each ticker's dataframe
        mfh = pd.DataFrame()

        for ticker in self.ticker_active:
            if ticker.mutualfund_holders is None:
                pass

            else:
                # get each mutual fund holder
                mutualfundholders = ticker.mutualfund_holders

                # concatenate into one dataframe
                mfh = pd.concat([mutualfundholders])

                # make dataframe format nicer
                # Swap dates and columns
                data = mfh.T
                # reset index (date) into a column
                data = data.reset_index()
                # Rename old index from '' to Date
                data.columns = ['Date', *data.columns[1:]]
                # Add ticker to dataframe
                data['Tickers'] = ticker.ticker
                dfs.append(data)

        parser = pd.io.parsers.base_parser.ParserBase({'usecols': None})

        for mutalFundHolders in dfs:
            mutalFundHolders.columns = parser._maybe_dedup_names(
                mutalFundHolders.columns)

        if len(dfs) > 0:
            mutalFundHolders = pd.concat(dfs, ignore_index=True)
            mutalFundHolders = mutalFundHolders.set_index(['Tickers', 'Date'])
        else:
            mutalFundHolders = pd.DataFrame(
                columns=['Holder', 'Shares', 'Date Reported', '% Out', 'Value'])
        return mutalFundHolders

    def getInstitutionalHolders(self):
        # Get Institutional holders
        dfs = []  # list for each ticker's dataframe
        ih = pd.DataFrame()

        for ticker in self.ticker_active:
            if ticker.mutualfund_holders is None:
                pass

            else:
                # get each institutional holder
                institutionalholder = ticker.mutualfund_holders

                # concatenate into one dataframe
                ih = pd.concat([institutionalholder])

                # make dataframe format nicer
                # Swap dates and columns
                data = ih.T
                # reset index (date) into a column
                data = data.reset_index()
                # Rename old index from '' to Date
                data.columns = ['Date', *data.columns[1:]]
                # Add ticker to dataframe
                data['Tickers'] = ticker.ticker
                dfs.append(data)

        parser = pd.io.parsers.base_parser.ParserBase({'usecols': None})

        for institutionalHolders in dfs:
            institutionalHolders.columns = parser._maybe_dedup_names(
                institutionalHolders.columns)

        if len(dfs) > 0:
            institutionalHolders = pd.concat(dfs, ignore_index=True)
            institutionalHolders = institutionalHolders.set_index(
                ['Tickers', 'Date'])
        else:
            institutionalHolders = pd.DataFrame(
                columns=['Holder', 'Shares', 'Date Reported', '% Out', 'Value'])
            return institutionalHolders

        def getDictionary(self, companyCode):
            dict = {}

            # Getting listed and delisted tickers
            listed, delisted = self.checkTickers(companyCode)
            dict['Listed'] = listed
            dict['Delisted'] = delisted

            # Getting listed tickers history data
            dict['Historical Data'] = self.getHistoricalData(listed)

            tickers = [yf.Ticker(ticker) for ticker in listed]
            # Getting Financial Statement
            dict['Financial Statement'] = self.getFinancialStatement()

            # Getting Quarterly Financial Statement
            dict['Quarterly Financial Statement'] = self.getQuarterlyFinancialStatement()

            # Getting ISIN code (International Securities Identification Number)
            dict['isin'] = self.getISINcode()

            # Getting tickers revenues and earnings
            revenues_and_earnings = self.getEarningsandRevenue()

            # Getting tickers quarterly revenues and earnings
            quarterly_revenues_and_earnings = self.getQuarterlyEarningsandRevenue()

            # Getting tickers major holders
            dict['Major Holders'] = self.getMajorHolders()

            # Getting tickers basic shares
            dict['Basic Shares'] = self.getBasicShares()

            # Getting tickers stock information
            dict['Stock Information'] = self.getStockInfo()

            # Getting tickers sustainability
            dict['sustainability'] = self.getSustainability()

            # Getting tickers calendar - next events
            dict['calendar'] = self.getCalendar()

            # Getting tickers recommendations
            dict['recommendations'] = self.getRecommendations()

            # Getting tickers analysis
            dict['analysis'] = self.getAnalysis()

            # Getting tickers Mutual Fund Holders
            dict['Mutal Fund Holders'] = self.getMutualFundHolders()

            # Getting tickers Institutional Holders
            dict['Institutional Holders'] = self.getInstitutionalHolders()
            return dict
