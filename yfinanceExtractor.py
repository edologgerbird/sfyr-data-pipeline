from datetime import datetime, timedelta
import datetime as dt
import pandas as pd
import numpy as np
import yfinance as yf
from urllib.request import Request, urlopen
import requests
import lxml
from functools import reduce


class yFinanceExtractor:
    def __init__(self, fileDirectory = None):
        if (fileDirectory == None):
            print("Future Integration with BigQuery")
        else:
            df = pd.read_csv()
            df.company_code = df.company_code.str[:] + ".SI"
    
    def tickerQuery(self,queryName, *args):
        query = getattr(yf, queryName)
    

    def dfToCSV(self, data, name):
        if isinstance(data, pd.DataFrame):
            filename = name + '.csv'
            data.to_csv(filename, index=False)
        else:
            dict = {'Name': data}
            dataframe = pd.DataFrame(dict)
            filename = name + '.csv'
            dataframe.to_csv(filename, index=False)
    
    def checkTickers(self,companyCode):
        # To check if ticker exist
        listed = []
        delisted = []

        for t in companyCode:
            length = yf.download(t, period='max', interval = '1d', timeout= None).shape[0]
            if length > 0:
                listed.append(t)
            else:
                delisted.append(t)

        # Generate CSV of existing and missing tickers
        dfTickers = pd.DataFrame({'Listed':listed})
        dfTickers["Delisted"] = pd.Series(delisted)
        dfToCSV(dfTickers, 'Tickers (Listed & Delisted)')
        return listed, delisted
        
    def getHistoricalData(self, listedTickerArray):
        # Listed Tickers' historical market data
        listedCodeList = " ".join(listedTickerArray)
        historicalData = yf.download(listedCodeList, period='max', interval = '1d', actions = True, timeout= None)

        # Generate all existing tickers' history
        dfToCSV(historicalData, 'Tickers historical market data')
        return historicalData
    
    def getFinancialStatement(self, tickers):
        # Retrieve financial statement (Financials, Balance Sheet and Cash flow)
        dfs = [] # list for each ticker's dataframe
        for ticker in tickers:
            # get each financial statement
            pnl = ticker.financials
            bs = ticker.balancesheet
            cf = ticker.cashflow

            # concatenate into one dataframe
            fs = pd.concat([pnl, bs, cf])

            # make dataframe format nicer
            # Swap dates and columns
            data = fs.T
            # reset index (date) into a column
            data = data.reset_index()
            # Rename old index from '' to Date
            data.columns = ['Date', *data.columns[1:]]
            # Add ticker to dataframe
            data['Ticker'] = ticker.ticker
            dfs.append(data)

        parser = pd.io.parsers.base_parser.ParserBase({'usecols': None})

        for fs in dfs:
                fs.columns = parser._maybe_dedup_names(fs.columns)
        fs = pd.concat(dfs, ignore_index=True)
        fs = fs.set_index(['Ticker','Date'])
        dfToCSV(fs, 'Financial Statement (financials, balance sheet, cashflow)')
        return fs
        
        def getQuarterlyFinancialStatement(self, tickers):
            # Get quarterly financial statement (Financials, Balance Sheet, Cashflows)
            dfs = [] # list for each ticker's dataframe
            for ticker in tickers:
                # get each quarter financial statement
                pnl = ticker.quarterly_financials
                bs = ticker.quarterly_balancesheet
                cf = ticker.quarterly_cashflow

                # concatenate into one dataframe
                fs = pd.concat([pnl, bs, cf])

                # make dataframe format nicer
                # Swap dates and columns
                data = fs.T
                # reset index (date) into a column
                data = data.reset_index()
                # Rename old index from '' to Date
                data.columns = ['Date', *data.columns[1:]]
                # Add ticker to dataframe
                data['Ticker'] = ticker.ticker
                dfs.append(data)

            parser = pd.io.parsers.base_parser.ParserBase({'usecols': None})

            for qfs in dfs:
                 qfs.columns = parser._maybe _dedup_names(qfs.columns)
            qfs = pd.concat(dfs, ignore_index=True)
            qfs = qfs.set_index(['Ticker','Date'])
            dfToCSV(qfs, 'Quarterly Financial Statement (financials, balance sheet, cashflow)')
            return qfs
        
        def getISINcode(self, tickers):
            # Get ISIN code (International Securities Identification Number)
            isin_dict = {}
            for ticker in tickers:
                try:
                    isin = ticker.isin
                    isin_dict[ticker.ticker] = isin
                except:
                    isin_dict[ticker.ticker] = np.nan

            df_isin = pd.DataFrame(list(isin_dict.items()),columns = ['Ticker','International Securities Identification Number'])
            dfToCSV(df_isin, 'ISIN Code')
            return df_isin
          
        # NEED TO REFORMAT THE INDEXING
        def getEarningsandRevenue(self, tickers):
            # Get Earnings and Revenue
            earnings = pd.DataFrame()
            revenues = pd.DataFrame()

            for ticker in tickers:
                if (ticker.earnings.shape[1] > 2):
                    revenues[ticker.ticker] = np.nan
                    earnings[ticker.ticker] = np.nan

                else:
                    earning = ticker.earnings.Earnings.rename(ticker.ticker).to_frame()
                    earnings = pd.concat([earnings,earning])
                    revenue = ticker.earnings.Revenue.rename(ticker.ticker).to_frame()
                    revenues = pd.concat([revenues,revenue])
            dfToCSV(revenues, 'Revenue')
            dfToCSV(earnings, 'Earning')
            return revenues, earnings
            
        # NEED TO REFORMAT THE INDEXING
        def getQuarterlyEarningsandRevenue(self, tickers):
            # Get Quarterly Earnings and Revenue
            qearnings = pd.DataFrame()
            qrevenues = pd.DataFrame()

            for ticker in tickers:
                if (ticker.quarterly_earnings.shape[1] > 2):
                    qrevenues[ticker.ticker] = np.nan
                    qearnings[ticker.ticker] = np.nan

                else:
                    qearning = ticker.quarterly_earnings.Earnings.rename(ticker.ticker).to_frame()
                    qearnings = pd.concat([qearnings,qearning])
                    qrevenue = ticker.quarterly_earnings.Revenue.rename(ticker.ticker).to_frame()
                    qrevenues = pd.concat([qrevenues,qrevenue])
            dfToCSV(qrevenues, 'Quarterly Revenue')
            dfToCSV(qearnings, 'Quarterly Earning')
            return qrevenues, qearnings
            
        def getMajorHolders(self, tickers):
            # Get Major Holders
            majorHolders = pd.DataFrame()
            for ticker in tickers:
                if ticker.major_holders is None or ticker.major_holders.shape[0] != 4:
                    data= {'% of Shares Held by All Insider': np.nan, '% of Shares Held by Institutions':np.nan ,
                            '% of Float Held by Institutions':np.nan,'Number of Institutions Holding Shares':np.nan,
                            'Ticker':ticker.ticker}
                    mHolders = pd.DataFrame(data, index = [count])
                    majorHolders = pd.concat([majorHolders,mHolders])

                else:
                    mHolders = ticker.major_holders[0].rename({0: '% of Shares Held by All Insider', 1:'% of Shares Held by Institutions' , 2:'% of Float Held by Institutions' ,3:'Number of Institutions Holding Shares'})
                    mHolders['Ticker'] = ticker.ticker
                    majorHolders = majorHolders.append(mHolders)
            majorHolders = majorHolders.reset_index(drop=True)
            dfToCSV(majorHolders, 'Major Holders')
            return majorHolders
        
        # NEED TO REFORMAT THE INDEXING
        def getBasicShares(self,tickers):
            # Get Basic Shares
            shares = pd.DataFrame()
            for ticker in tickers:
                if ticker.shares is None:
                    shares[ticker.ticker] = np.nan
                else:
                    share = ticker.shares.rename(columns = {"BasicShares": ticker.ticker})
                    shares = pd.concat([shares, share])

            dfToCSV(shares, 'Basic Shares')
            return shares
            
        # NEED TO REFORMAT THE INDEXING
        def getStockInfo(self, tickers):
            # Get stock information
            info = pd.DataFrame()

            for ticker in tickers:
                if ticker.info is None:
                    info[ticker.ticker] = np.nan
                else:
                    stockInfo = pd.DataFrame(ticker.info.values(), index = ticker.info.keys(), columns= [ticker.ticker])
                    info = pd.concat([info, stockInfo])
            dfToCSV(info, 'Stock Information')
            return info
        
        def getOptionsExpirations(self,tickers):
            # Get options expirations
            options_dict = {}

            for ticker in tickers:
                options_dict[ticker.ticker] = list(ticker.options)
                df_options = pd.DataFrame.from_dict(options_dict, orient = 'index').transpose()
            
            dfToCSV(df_options, 'Options Expiration')
            return df_options
        
        def getSustainability(self, tickers):
            # Get Sustainability

            sus = pd.DataFrame()

            for ticker in tickers:
                if ticker.sustainability is None:
                    sus[ticker.ticker] = np.nan
                else:
                    sustainability = ticker.sustainability.rename(columns={"Value": ticker.ticker})
                    sus = pd.concat([sus, sustainability])
            dfToCSV(sus, 'Sustainability')
            return sus
        
        def getCalendar(self,tickers):
        # Get next event (earnings, etc)
        calendar = pd.DataFrame()
        for ticker in tickers:
            if ticker.calendar is None:
                data= {'Earnings Date': np.nan,'Earnings Average': np.nan,'Earnings Low':np.nan,'Earnings High':np.nan,'Revenue Average':np.nan,
                                    'Revenue Low':np.nan,    'Revenue High':np.nan,    'Ticker':ticker.ticker}
                c = pd.DataFrame(data, index=['Value'])
                calendar = pd.concat([calendar,c])

            else:
                c = ticker.calendar.transpose()
                c['Ticker'] = ticker.ticker
                calendar = pd.concat([calendar,c])
            dfToCSV(calendar, 'Calendar (Next Events)')
            return calendar
        
        def getRecommendations(self,tickers):
            # Get Recommendations
            dfs = [] # list for each ticker's dataframe
            fs = pd.DataFrame()

            for ticker in tickers:
                if ticker.recommendations is None:
                    pass
                
                else:
                    # get each recommendations
                    re = ticker.recommendations

                    # concatenate into one dataframe
                    fs = pd.concat([re])

                    # make dataframe format nicer
                    # Swap dates and columns
                    data = fs.T
                    # reset index (date) into a column
                    data = data.reset_index()
                    # Rename old index from '' to Date
                    data.columns = ['Date', *data.columns[1:]]
                    # Add ticker to dataframe
                    data['Ticker'] = ticker.ticker
                    dfs.append(data)

            parser = pd.io.parsers.base_parser.ParserBase({'usecols': None})

            for reco in dfs:
                 reco.columns = parser._maybe_dedup_names(reco.columns)

            if len(dfs) > 0:
                reco = pd.concat(dfs, ignore_index=True)
                reco = reco.set_index(['Ticker','Date'])
            else:
                reco = pd.DataFrame(columns=['Firm', 'To Grade', 'From Grade', 'Action'])
            dfToCSV(reco, 'Recommendations')
            return reco
        
        def getAnalysis(self, tickers):
            # Get Analysis
            dfs = [] # list for each ticker's dataframe
            fs = pd.DataFrame()

            for ticker in tickers:
                if ticker.analysis is None:
                    pass
                
                else:
                    # get each analysis
                    an = ticker.analysis

                    # concatenate into one dataframe
                    fs = pd.concat([an])

                    # make dataframe format nicer
                    # Swap dates and columns
                    data = fs.T
                    # reset index (date) into a column
                    data = data.reset_index()
                    # Rename old index from '' to Date
                    data.columns = ['Date', *data.columns[1:]]
                    # Add ticker to dataframe
                    data['Ticker'] = ticker.ticker
                    dfs.append(data)

            parser = pd.io.parsers.base_parser.ParserBase({'usecols': None})

            for analysis in dfs:
                 analysis.columns = parser._maybe_dedup_names(analysis.columns)

            if len(dfs) > 0:
                analysis = pd.concat(dfs, ignore_index=True)
                analysis = analysis.set_index(['Ticker','Date'])
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
            dfToCSV(analysis, 'Analysis')
            return analysis
        
        # FUNCTION NOT WORKING???
        def getNews(self, tickers):
            pass
        
        def getMutualFundHolders(self, tickers):
            # Get Mutual Fund Holders
            dfs = [] # list for each ticker's dataframe
            fs = pd.DataFrame()

            for ticker in tickers:
                if ticker.mutualfund_holders is None:
                    pass
                
                else:
                    # get each mutual fund holder
                    mfh = ticker.mutualfund_holders

                    # concatenate into one dataframe
                    fs = pd.concat([mfh])

                    # make dataframe format nicer
                    # Swap dates and columns
                    data = fs.T
                    # reset index (date) into a column
                    data = data.reset_index()
                    # Rename old index from '' to Date
                    data.columns = ['Date', *data.columns[1:]]
                    # Add ticker to dataframe
                    data['Ticker'] = ticker.ticker
                    dfs.append(data)

            parser = pd.io.parsers.base_parser.ParserBase({'usecols': None})

            for mutalFundHolders in dfs:
                 mutalFundHolders.columns = parser._maybe_dedup_names(mutalFundHolders.columns)
                 
            if len(dfs) > 0:
                mutalFundHolders = pd.concat(dfs, ignore_index=True)
                mutalFundHolders = mutalFundHolders.set_index(['Ticker','Date'])
            else:
                mutalFundHolders = pd.DataFrame(columns=['Holder', 'Shares', 'Date Reported', '% Out', 'Value'])
            dfToCSV(mutalFundHolders, 'Mutual Fund Holders')
            return mutalFundHolders
        
        def getInstitutionalHolders(self,tickers):
            # Get Institutional holders
            dfs = [] # list for each ticker's dataframe
            fs = pd.DataFrame()

            for ticker in tickers:
                if ticker.mutualfund_holders is None:
                    pass
                
                else:
                    # get each institutional holder
                    ih = ticker.mutualfund_holders

                    # concatenate into one dataframe
                    fs = pd.concat([ih])

                    # make dataframe format nicer
                    # Swap dates and columns
                    data = fs.T
                    # reset index (date) into a column
                    data = data.reset_index()
                    # Rename old index from '' to Date
                    data.columns = ['Date', *data.columns[1:]]
                    # Add ticker to dataframe
                    data['Ticker'] = ticker.ticker
                    dfs.append(data)

            parser = pd.io.parsers.base_parser.ParserBase({'usecols': None})

            for institutionalHolders in dfs:
                 institutionalHolders.columns = parser._maybe_dedup_names(institutionalHolders.columns)

            if len(dfs) > 0:
                institutionalHolders = pd.concat(dfs, ignore_index=True)
                institutionalHolders = institutionalHolders.set_index(['Ticker','Date'])
            else:
                institutionalHolders = pd.DataFrame(columns=['Holder', 'Shares', 'Date Reported', '% Out', 'Value'])
                dfToCSV(institutionalHolders, 'Institutional Holders')
                return institutionalHolders
    def main():
        # Reading data
        file = 'SGX_data.csv'
        df = pd.read_csv(file)
        
        #Extracting Company Code
        companyCode = df['company_code'].to_numpy()
        
        # Getting listed and delisted tickers
        listed, delisted = yfinanceExtractor().checkTickers(companyCode)
        
        # Getting listed tickers history data
        getHistoricalData = yfinanceExtractor().getHistoricalData(listed)
        
        tickers = [yf.Ticker(ticker) for ticker in listed]
        
        # Getting Financial Statement
        fs = yfinanceExtractor().getFinancialStatement(tickers)
        
        # Getting Quarterly Financial Statement
        qfs = yfinanceExtractor().getQuarterlyFinancialStatement(tickers)
        
        # Getting ISIN code (International Securities Identification Number)
        isin = yfinanceExtractor().getISINcode(tickers)
        
        #Getting tickers revenues and earnings
        revenues, earnings = yfinanceExtractor().getEarningsandRevenue(tickers)
        
        #Getting tickers quarterly revenues and earnings
        qrevenues, qearnings = yfinanceExtractor().getQuarterlyEarningsandRevenue(tickers)
        
        # Getting tickers major holders
        majorHolders = yfinanceExtractor().getMajorHolders(tickers)
        
        # Getting tickers basic shares
        shares = yfinanceExtractor().getBasicShares(tickers)
       
        # Getting tickers stock information
        info = yfinanceExtractor().getStockInfo(tickers)
        
        # Getting Tickers options expirations
        optionsExpiration = yfinanceExtractor().getOptionsExpirations(tickers)
        
        # Getting tickers sustainability
        sustainability  = yfinanceExtractor().getSustainability(tickers)
        
        # Getting tickers calendar - next events
        calendar = yfinanceExtractor().getCalendar(tickers)
        
        # Getting tickers recommendations
        reco = yfinanceExtractor().getRecommendations(tickers)
        
        # Getting tickers analysis
        analysis = yfinanceExtractor().getAnalysis(tickers)
        
        # Getting tickers news
        # NOT WORKING
        news = yfinanceExtractor().getNews(tickers)
        
        # Getting tickers Mutual Fund Holders
        mutalFundHolders = yfinanceExtractor().getMutualFundHolders(tickers)
        
        # Getting tickers Institutional Holders
        institutionalHolders = yfinanceExtractor().getInstitutionalHolders(tickers)
    main()
    
    
