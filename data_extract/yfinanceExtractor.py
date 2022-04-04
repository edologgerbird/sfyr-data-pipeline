from datetime import datetime as dt
from datetime import timedelta
import pandas as pd
import numpy as np
import yfinance as yf
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

        # Generate dataframe of existing and missing tickers
        dfTickers = pd.DataFrame({'Listed':listed})
        dfTickers["Delisted"] = pd.Series(delisted)
        return listed, delisted
        
    def getHistoricalData(self, listedTickerArray):
        # Listed Tickers' historical market data
        listedCodeList = " ".join(listedTickerArray)
        historicalData = yf.download(listedCodeList, period='max', interval = '1d', actions = True, timeout= None)

        # Generate all existing tickers' history
        return historicalData
    

    def getFinancialStatement(self, tickers):
        # Retrieve financial statement (Financials, Balance Sheet and Cash flow)
        dfs = [] # list for each ticker's dataframe
        fs = pd.DataFrame()
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
        return fs
        
    def getQuarterlyFinancialStatement(self, tickers):
        # Get quarterly financial statement (Financials, Balance Sheet, Cashflows)
        dfs = [] # list for each ticker's dataframe
        fs = pd.DataFrame()
        for ticker in tickers:
            # get each quarter financial statement
            pnl = ticker.quarterly_financials
            bs = ticker.quarterly_balancesheet
            cf = ticker.quarterly_cashflow

            # concatenate into one dataframe
            qfs = pd.concat([pnl, bs, cf])

            # make dataframe format nicer
            # Swap dates and columns
            data = qfs.T
            # reset index (date) into a column
            data = data.reset_index()
            # Rename old index from '' to Date
            data.columns = ['Date', *data.columns[1:]]
            # Add ticker to dataframe
            data['Ticker'] = ticker.ticker
            dfs.append(data)

        parser = pd.io.parsers.base_parser.ParserBase({'usecols': None})

        for qfs in dfs:
                qfs.columns = parser._maybe_dedup_name(qfs.columns)
        qfs = pd.concat(dfs, ignore_index=True)
        qfs = qfs.set_index(['Ticker','Date'])
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
        return qrevenues, qearnings
            
    def getMajorHolders(self, tickers):
        # Get Major Holders
        majorHolders = pd.DataFrame()
        for ticker in tickers:
            if ticker.major_holders is None or ticker.major_holders.shape[0] != 4:
                data= {'% of Shares Held by All Insider': np.nan, '% of Shares Held by Institutions':np.nan ,
                        '% of Float Held by Institutions':np.nan,'Number of Institutions Holding Shares':np.nan,
                        'Ticker':ticker.ticker}
                mHolders = pd.DataFrame(data, index = [0])
                majorHolders = pd.concat([majorHolders,mHolders])

            else:
                mHolders = ticker.major_holders[0].rename({0: '% of Shares Held by All Insider', 1:'% of Shares Held by Institutions' , 2:'% of Float Held by Institutions' ,3:'Number of Institutions Holding Shares'})
                mHolders['Ticker'] = ticker.ticker
                majorHolders = majorHolders.append(mHolders)
        majorHolders = majorHolders.reset_index(drop=True)
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
        return info
        
    def getOptionsExpirations(self,tickers):
        # Get options expirations
        options_dict = {}

        for ticker in tickers:
            options_dict[ticker.ticker] = list(ticker.options)
            df_options = pd.DataFrame.from_dict(options_dict, orient = 'index').transpose()
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
            return calendar
    
    def getRecommendations(self,tickers):
        # Get Recommendations
        dfs = [] # list for each ticker's dataframe
        re = pd.DataFrame()

        for ticker in tickers:
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
        return reco
        
    def getAnalysis(self, tickers):
        # Get Analysis
        dfs = [] # list for each ticker's dataframe
        an = pd.DataFrame()

        for ticker in tickers:
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
        return analysis
        
    # FUNCTION NOT WORKING???
    def getNews(self, tickers):
        pass
    
    def getMutualFundHolders(self, tickers):
        # Get Mutual Fund Holders
        dfs = [] # list for each ticker's dataframe
        mfh = pd.DataFrame()

        for ticker in tickers:
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
        return mutalFundHolders

      
    def getInstitutionalHolders(self,tickers):
        # Get Institutional holders
        dfs = [] # list for each ticker's dataframe
        ih = pd.DataFrame()

        for ticker in tickers:
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
            return institutionalHolders
        
        def getDictionary(self, companyCode):
            dict = {}

            # Getting listed and delisted tickers
            listed, delisted = yFinanceExtractor().checkTickers(companyCode)
            dict['Listed'] = listed
            dict['Delisted'] = delisted

            # Getting listed tickers history data
            historicalData = yFinanceExtractor().getHistoricalData(listed)
            dict['Historical Data'] = historicalData

            tickers = [yf.Ticker(ticker) for ticker in listed]
            # Getting Financial Statement
            fs = yFinanceExtractor().getFinancialStatement(tickers)
            dict['Financial Statement'] = fs

            # Getting Quarterly Financial Statement
            qfs = yFinanceExtractor().getQuarterlyFinancialStatement(tickers)
            dict['Quarterly Financial Statement'] = qfs

            # Getting ISIN code (International Securities Identification Number)
            isin = yFinanceExtractor().getISINcode(tickers)
            dict['isin'] = isin

            #Getting tickers revenues and earnings
            revenues, earnings = yFinanceExtractor().getEarningsandRevenue(tickers)
            dict['revenues'] = revenues
            dict['earnings'] = earnings

            #Getting tickers quarterly revenues and earnings
            qrevenues, qearnings = yFinanceExtractor().getQuarterlyEarningsandRevenue(tickers)
            dict['quarterly revenues'] = qrevenues
            dict['quarterly earnings'] = qearnings

            # Getting tickers major holders
            majorHolders = yFinanceExtractor().getMajorHolders(tickers)
            dict['Major Holders'] = majorHolders
            
            # Getting tickers basic shares
            shares = yFinanceExtractor().getBasicShares(tickers)
            dict['Basic Shares'] = shares

            # Getting tickers stock information
            info = yFinanceExtractor().getStockInfo(tickers)
            dict['Stock Information'] = info

            # Getting Tickers options expirations
            optionsExpiration = yFinanceExtractor().getOptionsExpirations(tickers)
            dict['Options Expiration'] = optionsExpiration

            # Getting tickers sustainability
            sustainability  = yFinanceExtractor().getSustainability(tickers)
            dict['sustainability'] = sustainability

            # Getting tickers calendar - next events
            calendar = yFinanceExtractor().getCalendar(tickers)
            dict['calendar'] =calendar

            # Getting tickers recommendations
            reco = yFinanceExtractor().getRecommendations(tickers)
            dict['recommendations'] = reco

            # Getting tickers analysis
            analysis = yFinanceExtractor().getAnalysis(tickers)
            dict['analysis'] = analysis

            # Getting tickers news
            # NOT WORKING
            news = yFinanceExtractor().getNews(tickers)
            dict['news'] = news

            # Getting tickers Mutual Fund Holders
            mutalFundHolders = yFinanceExtractor().getMutualFundHolders(tickers)
            dict['Mutal Fund Holders'] = mutalFundHolders
            
            # Getting tickers Institutional Holders
            institutionalHolders = yFinanceExtractor().getInstitutionalHolders(tickers)
            dict['Institutional Holders'] = institutionalHolders
            return dict
def main():
        # Reading data
        file = 'SGX_data.csv'
        df = pd.read_csv(file)
        
        #Extracting Company Code
        companyCode = df['company_code'].to_numpy()
        dict = yFinanceExtractor().getDictionary(companyCode)
main()
    
    
