from datetime import datetime as dt
import pandas as pd
import numpy as np
import yfinance as yf
import time
from functools import reduce
import asyncio


class yfinanceExtractor:
    def __init__(self, sgxTickers):
        self.sgxTickers = sgxTickers
        self.sgxTickers.ticker = self.sgxTickers.ticker.str[:] + ".SI"

        # Initalisation of Shared Data
        self.ticker_active = []  # List of Active Ticker Objects
        self.ticker_delisted = []  # List of Inactive Ticker Name String

        # yFinance Ouput
        self.yfinanceData = {
            "ticker_status": pd.DataFrame(columns=["Listed", "Delisted"]),
            "historical_data": pd.DataFrame(columns=['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume', 'Tickers', 'Market Status']),
            "financial_statements": pd.DataFrame(columns=['Date', 'Research Development', 'Effect Of Accounting Charges', 'Income Before Tax', 'Minority Interest', 'Net Income_x', 'Selling General Administrative', 'Gross Profit', 'Ebit', 'Operating Income', 'Other Operating Expenses', 'Interest Expense', 'Extraordinary Items', 'Non Recurring', 'Other Items', 'Income Tax Expense', 'Total Revenue', 'Total Operating Expenses', 'Cost Of Revenue', 'Total Other Income Expense Net', 'Discontinued Operations', 'Net Income From Continuing Ops', 'Net Income Applicable To Common Shares', 'Total Liab', 'Total Stockholder Equity', 'Other Current Liab', 'Total Assets', 'Common Stock', 'Other Current Assets', 'Retained Earnings', 'Other Liab', 'Cash', 'Total Current Liabilities', 'Property Plant Equipment', 'Total Current Assets', 'Net Tangible Assets', 'Net Receivables', 'Accounts Payable', 'Intangible Assets', 'Treasury Stock', 'Other Assets', 'Other Stockholder Equity', 'Long Term Debt', 'Deferred Long Term Asset Charges', 'Change To Liabilities', 'Net Borrowings', 'Total Cash From Financing Activities', 'Net Income_y', 'Change In Cash', 'Total Cash From Operating Activities', 'Depreciation', 'Change To Account Receivables', 'Change To Netincome', 'Total Cashflows From Investing Activities', 'Capital Expenditures', 'Change To Operating Activities', 'Issuance Of Stock', 'Minority Interest_x', 'Minority Interest_y', 'Short Long Term Debt', 'Inventory', 'Good Will', 'Other Cashflows From Investing Activities', 'Change To Inventory', 'Other Cashflows From Financing Activities', 'Short Term Investments', 'Effect Of Exchange Rate', 'Long Term Investments', 'Investments', 'Dividends Paid', 'Repurchase Of Stock', 'Capital Surplus', 'Deferred Long Term Liab']),
            "quarterly_financial_statements": pd.DataFrame(columns=['Date', 'Research Development', 'Effect Of Accounting Charges', 'Income Before Tax', 'Minority Interest', 'Net Income_x', 'Selling General Administrative', 'Gross Profit', 'Ebit', 'Operating Income', 'Other Operating Expenses', 'Interest Expense', 'Extraordinary Items', 'Non Recurring', 'Other Items', 'Income Tax Expense', 'Total Revenue', 'Total Operating Expenses', 'Cost Of Revenue', 'Total Other Income Expense Net', 'Discontinued Operations', 'Net Income From Continuing Ops', 'Net Income Applicable To Common Shares', 'Total Liab', 'Total Stockholder Equity', 'Other Current Liab', 'Total Assets', 'Common Stock', 'Other Current Assets', 'Retained Earnings', 'Other Liab', 'Cash', 'Total Current Liabilities', 'Property Plant Equipment', 'Total Current Assets', 'Net Tangible Assets', 'Net Receivables', 'Accounts Payable', 'Intangible Assets', 'Treasury Stock', 'Other Assets', 'Other Stockholder Equity', 'Long Term Debt', 'Net Income_y', 'Change To Liabilities', 'Net Borrowings', 'Total Cash From Financing Activities', 'Change In Cash', 'Total Cash From Operating Activities', 'Depreciation', 'Change To Account Receivables', 'Change To Netincome', 'Minority Interest_x', 'Minority Interest_y', 'Deferred Long Term Asset Charges', 'Short Long Term Debt', 'Inventory', 'Total Cashflows From Investing Activities', 'Change To Operating Activities', 'Issuance Of Stock', 'Other Cashflows From Investing Activities', 'Change To Inventory', 'Other Cashflows From Financing Activities', 'Capital Expenditures', 'Short Term Investments', 'Effect Of Exchange Rate', 'Long Term Investments', 'Investments', 'Good Will', 'Repurchase Of Stock', 'Dividends Paid', 'Capital Surplus', 'Deferred Long Term Liab']),
            "earnings_and_revenue": pd.DataFrame(columns=['Year', 'Revenue', 'Earnings', 'Tickers']),
            "quarterly_earnings_and_revenue": pd.DataFrame(columns=['Quarters', 'Revenue', 'Earnings', 'Tickers']),
            "majorHolders": pd.DataFrame(columns=["Tickers", '% of Shares Held by All Insider', '% of Shares Held by Institutions',
                                                  '% of Float Held by Institutions', 'Number of Institutions Holding Shares'], dtype="string"),
            "basic_shares": pd.DataFrame(columns=['Year', 'Tickers']),
            "stock_info": pd.DataFrame(columns=['Tickers', '52WeekChange', 'SandP52WeekChange', 'address1', 'address2', 'algorithm', 'annualHoldingsTurnover', 'annualReportExpenseRatio', 'ask', 'askSize', 'averageDailyVolume10Day', 'averageDailyVolume3Month',
                                                'averageVolume', 'averageVolume10days', 'beta', 'beta3Year', 'bid', 'bidSize', 'bondHoldings', 'bondPosition', 'bondRatings', 'bookValue', 'cashPosition', 'category', 'circulatingSupply', 'city',
                                                'companyOfficers', 'convertiblePosition', 'country', 'currency', 'currencySymbol', 'currentPrice', 'currentRatio', 'dateShortInterest', 'dayHigh', 'dayLow', 'debtToEquity', 'dividendRate', 'dividendYield',
                                                'earningsGrowth', 'earningsQuarterlyGrowth', 'ebitda', 'ebitdaMargins', 'enterpriseToEbitda', 'enterpriseToRevenue', 'enterpriseValue', 'equityHoldings', 'err', 'exDividendDate', 'exchange', 'exchangeDataDelayedBy',
                                                'exchangeName', 'exchangeTimezoneName', 'exchangeTimezoneShortName', 'expireDate', 'fax', 'fiftyDayAverage', 'fiftyTwoWeekHigh', 'fiftyTwoWeekLow', 'financialCurrency', 'fiveYearAverageReturn', 'fiveYearAvgDividendYield',
                                                'floatShares', 'forwardEps', 'forwardPE', 'freeCashflow', 'fromCurrency', 'fullTimeEmployees', 'fundFamily', 'fundInceptionDate', 'gmtOffSetMilliseconds', 'grossMargins', 'grossProfits', 'headSymbol',
                                                'heldPercentInsiders', 'heldPercentInstitutions', 'holdings', 'industry', 'isEsgPopulated', 'lastCapGain', 'lastDividendDate', 'lastDividendValue', 'lastFiscalYearEnd', 'lastMarket', 'lastSplitDate', 'lastSplitFactor',
                                                'legalType', 'logo_url', 'longBusinessSummary', 'longName', 'market', 'marketCap', 'marketState', 'maxAge', 'maxSupply', 'messageBoardId', 'morningStarOverallRating', 'morningStarRiskRating', 'mostRecentQuarter',
                                                'navPrice', 'netIncomeToCommon', 'nextFiscalYearEnd', 'numberOfAnalystOpinions', 'open', 'openInterest', 'operatingCashflow', 'operatingMargins', 'otherPosition', 'payoutRatio', 'pegRatio',
                                                'phone', 'postMarketChange', 'postMarketPrice', 'preMarketChange', 'preMarketPrice', 'preferredPosition', 'previousClose', 'priceHint', 'priceToBook', 'priceToSalesTrailing12Months', 'profitMargins',
                                                'quickRatio', 'quoteSourceName', 'quoteType', 'recommendationKey', 'recommendationMean', 'regularMarketChange', 'regularMarketChangePercent', 'regularMarketDayHigh', 'regularMarketDayLow', 'regularMarketOpen',
                                                'regularMarketPreviousClose', 'regularMarketPrice', 'regularMarketSource', 'regularMarketTime', 'regularMarketVolume', 'returnOnAssets', 'returnOnEquity', 'revenueGrowth', 'revenuePerShare', 'revenueQuarterlyGrowth',
                                                'sector', 'sectorWeightings', 'sharesOutstanding', 'sharesPercentSharesOut', 'sharesShort', 'sharesShortPreviousMonthDate', 'sharesShortPriorMonth', 'shortName', 'shortPercentOfFloat', 'shortRatio',
                                                'startDate', 'state', 'stockPosition', 'strikePrice', 'symbol', 'targetHighPrice', 'targetLowPrice', 'targetMeanPrice', 'targetMedianPrice', 'threeYearAverageReturn', 'toCurrency', 'totalAssets',
                                                'totalCash', 'totalCashPerShare', 'totalDebt', 'totalRevenue', 'tradeable', 'trailingAnnualDividendRate', 'trailingAnnualDividendYield', 'trailingEps', 'trailingPE', 'twoHundredDayAverage', 'underlyingExchangeSymbol',
                                                'underlyingSymbol', 'uuid', 'volume', 'volume24Hr', 'volumeAllCurrencies', 'website', 'yield', 'ytdReturn', 'zip']),
            "stock_industry": pd.DataFrame(columns=['ticker', 'industry']),
            "stock_calendar": pd.DataFrame(columns=['Earnings Date', 'Earnings Average', 'Earnings Low', 'Earnings High', 'Revenue Average', 'Revenue Low', 'Revenue High', 'Tickers']),
            "stock_recommendation": pd.DataFrame(columns=["Date", "Tickers", "Firm", "To Grade", "From Grade", "Action"]),
            "stock_analysis": pd.DataFrame(columns=['Period', 'Max Age', 'End Date', 'Growth', 'Earnings Estimate Avg', 'Earnings Estimate Low', 'Earnings Estimate High', 'Earnings Estimate Year Ago Eps', 'Earnings Estimate Number Of Analysts', 'Earnings Estimate Growth', 'Revenue Estimate Avg', 'Revenue Estimate Low', 'Revenue Estimate High', 'Revenue Estimate Number Of Analysts', 'Revenue Estimate Year Ago Revenue', 'Revenue Estimate Growth', 'Eps Trend Current', 'Eps Trend 7Days Ago', 'Eps Trend 30Days Ago', 'Eps Trend 60Days Ago', 'Eps Trend 90Days Ago', 'Eps Revisions Up Last7Days', 'Eps Revisions Up Last30Days', 'Eps Revisions Down Last30Days', 'Eps Revisions Down Last90Days', 'Tickers']),
            "stock_mfh": pd.DataFrame(columns=['Tickers', 'Holder', 'Shares', 'Date Reported', '% Out', 'Value']),
            "stock_ih": pd.DataFrame(columns=['Tickers', 'Holder', 'Shares', 'Date Reported', '% Out', 'Value'])
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
        historical_data_df = self.yfinanceData["historical_data"]
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
        financial_statements_df = self.yfinanceData["financial_statements"]
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
        quarterly_financial_statements_df = self.yfinanceData["quarterly_financial_statements"]
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
        quarterly_financial_statements_df = self.yfinanceData["quarterly_financial_statements"]
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

    def getEarningsandRevenue(self):
        # Get Earnings and Revenue
        earnings_and_revenues_df = self.yfinanceData["earnings_and_revenue"]

        for ticker in self.ticker_active:
            if (ticker.earnings.shape[0] >= 1):
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
        quarterly_earnings_and_revenues_df = self.yfinanceData["quarterly_earnings_and_revenue"]

        for ticker in self.ticker_active:
            if (ticker.quarterly_earnings.shape[0] >= 1):
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
        majorHolders_df = self.yfinanceData["majorHolders"]
        for ticker in self.ticker_active:
            if ticker.major_holders is not None and ticker.major_holders.shape[0] == 4:
                # print(ticker.major_holders)
                ticker_majorHolders = ticker.major_holders.set_index(1)
                # print(ticker_majorHolders)
                ticker_majorHolders = ticker_majorHolders.T
                ticker_majorHolders["Tickers"] = self.removeSI(ticker.ticker)
                # print(ticker_majorHolders)
                ticker_majorHolders[:] = ticker_majorHolders[:].astype(
                    'string')
                print(majorHolders_df.dtypes)
                majorHolders_df = majorHolders_df.merge(
                    ticker_majorHolders, how="outer", on=list(ticker_majorHolders.columns))
                print(majorHolders_df)
                print(majorHolders_df.dtypes)
        self.yfinanceData["majorHolders"] = majorHolders_df
        return majorHolders_df

    def getBasicShares(self):
        # Get Basic Shares
        basic_shares_df = self.yfinanceData["basic_shares"]
        for ticker in self.ticker_active:
            if ticker.shares is not None:
                ticker_share = ticker.shares
                ticker_share['Tickers'] = self.removeSI(ticker.ticker)
                basic_shares_df = pd.concat(
                    [basic_shares_df, ticker_share])
        basic_shares_df = basic_shares_df.reset_index()
        # Store to Shared Data
        basic_shares_df = basic_shares_df.rename(
            columns={basic_shares_df.columns[0]: "Year"})
        self.yfinanceData["basic_shares"] = basic_shares_df
        return basic_shares_df

    def cast_dict_to_string(self, entry):
        if len(str(entry)) > 0 and str(entry)[0] in ["{", "["]:
            return str(entry)
        else:
            return entry

    def getStockInfo(self):
        # Get stock information
        all_tickers_info = self.yfinanceData["stock_info"]
        for ticker in self.ticker_active:
            if ticker.info is not None:
                print(ticker.info)
                ticker_info = pd.DataFrame(ticker.info.items()).set_index(0).T
                ticker_info["Tickers"] = self.removeSI(ticker.ticker)
                ticker_info["Tickers"] = ticker_info["Tickers"].astype(
                    "string")
                all_tickers_info = pd.concat([all_tickers_info, ticker_info])

        all_tickers_info = all_tickers_info.reset_index(drop=True)

        for column in all_tickers_info.columns:
            all_tickers_info[column] = all_tickers_info[column].apply(
                self.cast_dict_to_string)

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
        stock_calendar_df = self.yfinanceData["stock_calendar"]
        for ticker in self.ticker_active:
            if ticker.calendar is not None:
                ticker_calendar = ticker.calendar.transpose()
                ticker_calendar = ticker_calendar.rename(
                    columns={"Ticker": "Tickers"})
                ticker_calendar['Tickers'] = self.removeSI(ticker.ticker)
                ticker_calendar['Earnings Date'] = ticker_calendar['Earnings Date'].astype(
                    'datetime64[ns]')
                stock_calendar_df = pd.concat(
                    [stock_calendar_df, ticker_calendar])
        stock_calendar_df = stock_calendar_df.reset_index(drop=True)
        # Store to Shared Data
        print(stock_calendar_df.dtypes)
        self.yfinanceData["stock_calendar"] = stock_calendar_df
        return stock_calendar_df

    def getRecommendations(self):
        # Get Recommendations
        recommendations_df = self.yfinanceData["stock_recommendation"]

        for ticker in self.ticker_active:
            if ticker.recommendations is not None:
                ticker_recommendations = ticker.recommendations
                ticker_recommendations['Tickers'] = self.removeSI(
                    ticker.ticker)
                ticker_recommendations["Date"] = ticker_recommendations.index
                recommendations_df = pd.concat(
                    [recommendations_df, ticker_recommendations])
        recommendations_df = recommendations_df.reset_index(drop=True)

        # Store to Shared Data
        self.yfinanceData["stock_recommendation"] = recommendations_df
        return recommendations_df

    def getAnalysis(self):
        # Get Analysis
        analysis_df = self.yfinanceData["stock_analysis"]
        for ticker in self.ticker_active:
            if ticker.analysis is not None:
                ticker_analysis = ticker.analysis
                ticker_analysis['Tickers'] = self.removeSI(ticker.ticker)
                ticker_analysis['Tickers'] = ticker_analysis['Tickers'].astype(
                    "string")
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
        mfh_pd = self.yfinanceData["stock_mfh"]
        for ticker in self.ticker_active:
            if ticker.mutualfund_holders is not None:
                ticker_mfh = ticker.mutualfund_holders
                ticker_mfh['Tickers'] = self.removeSI(ticker.ticker)
                mfh_pd = pd.concat([mfh_pd, ticker_mfh])

        mfh_pd = mfh_pd.reset_index(drop=True)

        # Store to Shared Data
        self.yfinanceData["stock_mfh"] = mfh_pd
        return mfh_pd

    def getInstitutionalHolders(self):
        # Get Institutional holders
        ih_pd = self.yfinanceData["stock_ih"]

        for ticker in self.ticker_active:
            if ticker.institutional_holders is not None and ticker.institutional_holders.shape[1] == 5:
                ticker_ih = ticker.institutional_holders
                ticker_ih['Tickers'] = self.removeSI(ticker.ticker)
                ih_pd = pd.concat([ih_pd, ticker_ih])

        ih_pd = ih_pd.reset_index(drop=True)

        # Store to Shared Data
        self.yfinanceData["stock_ih"] = ih_pd

        return ih_pd

    def yfinanceQuery(self):
        failed = []
        # try:
        #     print("Query Historical Data")
        #     self.getHistoricalData()
        #     print("Historical Data Query Complete")
        # except:
        #     failed.append("Historical Data")

        # try:
        #     print("Query Financial Statement")
        #     self.getFinancialStatement()
        #     print("Financial Statement Query Complete")
        # except:
        #     failed.append("Financial Statement")

        # try:
        #     print(">> ========== START: Quarterly Financial Statement Query")
        #     print(self.getQuarterlyFinancialStatement())
        #     print(">> ========== COMPLETE: Quarterly Financial Statement Query")
        # except:
        #     failed.append("Quaterly Financial Statement")

        # try:
        #     print("Query Earnings and Revenue")
        #     self.getEarningsandRevenue()
        #     print("Earnings and Revenue Query Complete")
        # except:
        #     failed.append("Earnings and Revenue")

        # try:
        #     print("Query Quarterly Earnings and Revenue")
        #     self.getQuarterlyEarningsandRevenue()
        #     print("Quarterly Earnings and Revenue Query Complete")
        # except:
        #     failed.append("Quarterly Earnings and Revenue")

        # try:
        #     print(">> ========== START: Major Holders Query")
        #     self.getMajorHolders()
        #     print(">> ========== COMPLETE: Major Holders Query")
        # except:
        #     failed.append("Major Holders")

        # try:
        #     print(">> ========== START: Basic Shares Query")
        #     self.getBasicShares()
        #     print(">> ========== COMPLETE: Basic Shares Query")
        # except:
        #     failed.append("Basic Shares")

        try:
            print("Query Stock Info")
            self.getStockInfo()
            print("Stock Info Query Complete")
        except:
            failed.append("Stock Info")

        # try:
        #     print(">> ========== START: Stock Industry Extraction")
        #     self.getStockIndustry()
        #     print(">> ========== COMPLETE: Stock Industry Extraction")
        # except:
        #     failed.append("Stock industry")

        try:
            print(">> ========== START: Stock Calendar Query")
            self.getCalendar()
            print(">> ========== COMPLETE: Stock Calendar Query")
        except:
            failed.append("Calendar Query")

        # try:
        #     print(">> ========== START: Analyst Recommendations Query")
        #     self.getRecommendations()
        #     print(">> ========== COMPLETE: Analyst Recommendations Query")
        # except:
        #     failed.append("Analyst Recommendations")

        # try:
        #     print(">> ========== START: Stock Analysis Query")
        #     self.getAnalysis()
        #     print(">> ========== COMPLETE: Stock Analysis Query")
        # except:
        #     failed.append("Stock Analysis")

        # try:
        #     print(">> ========== START: Mutual Fund Holders Query")
        #     self.getMutualFundHolders()
        #     print(">> ========== COMPLETE: Mutual Fund Holders Query")
        # except:
        #     failed.append("Mutual Fund Holder")

        # try:
        #     print(">> ========== START: Institutional Holders Querys")
        #     self.getInstitutionalHolders()
        #     print(">> ========== COMPLETE: Institutional Holders Query")
        # except:
        #     failed.append("Institutional Holders")

        for name, df in self.yfinanceData.items():
            df.to_csv(f"output_store/{name}.csv", index=False)

        failed_df = pd.DataFrame(failed)
        failed_df.to_csv("output_store/failed.csv", index=False)

        for failure in failed:
            print(failure)

        return self.yfinanceData
