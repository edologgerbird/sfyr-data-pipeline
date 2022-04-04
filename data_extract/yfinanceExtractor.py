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
        all_tickers_earnings = pd.DataFrame()
        all_tickers_revenues = pd.DataFrame()

        for ticker in tickers:
            if (ticker.earnings.shape[1] > 2):
                all_tickers_revenues[ticker.ticker] = np.nan
                all_tickers_earnings[ticker.ticker] = np.nan

            else:
                ticker_earning = ticker.earnings.Earnings.rename(ticker.ticker).to_frame()
                all_tickers_earnings = pd.concat([all_tickers_earnings,ticker_earning])
                ticker_revenues = ticker.earnings.Revenue.rename(ticker.ticker).to_frame()
                all_tickers_revenues = pd.concat([all_tickers_revenues,ticker_revenues])
        return all_tickers_revenues, all_tickers_earnings
            
    # NEED TO REFORMAT THE INDEXING
    def getQuarterlyEarningsandRevenue(self, tickers):
        # Get Quarterly Earnings and Revenue
        all_tickers_quarterly_earnings = pd.DataFrame()
        all_tickers_quarterly_revenues = pd.DataFrame()

        for ticker in tickers:
            if (ticker.quarterly_earnings.shape[1] > 2):
                all_tickers_quarterly_revenues[ticker.ticker] = np.nan
                all_tickers_quarterly_earnings[ticker.ticker] = np.nan

            else:
                ticker_quarterly_earning = ticker.quarterly_earnings.Earnings.rename(ticker.ticker).to_frame()
                all_tickers_quarterly_earnings = pd.concat([all_tickers_quarterly_earnings,ticker_quarterly_earning])
                
                ticker_quarterly_revenue = ticker.quarterly_earnings.Revenue.rename(ticker.ticker).to_frame()
                all_tickers_quarterly_revenues = pd.concat([all_tickers_quarterly_revenues,ticker_quarterly_revenue])
        return all_tickers_quarterly_earnings, all_tickers_quarterly_revenues
            
    def getMajorHolders(self, tickers):
        # Get Major Holders
        all_tickers_majorHolders = pd.DataFrame()
        for ticker in tickers:
            if ticker.major_holders is None or ticker.major_holders.shape[0] != 4:
                data= {'% of Shares Held by All Insider': np.nan, '% of Shares Held by Institutions':np.nan ,
                        '% of Float Held by Institutions':np.nan,'Number of Institutions Holding Shares':np.nan,
                        'Ticker':ticker.ticker}
                ticker_majorHolders = pd.DataFrame(data, index = [0])
                all_tickers_majorHolders = pd.concat([all_tickers_majorHolders,ticker_majorHolders])

            else:
                ticker_majorHolders = ticker.major_holders[0].rename({0: '% of Shares Held by All Insider', 1:'% of Shares Held by Institutions' , 2:'% of Float Held by Institutions' ,3:'Number of Institutions Holding Shares'})
                ticker_majorHolders['Ticker'] = ticker.ticker
                all_tickers_majorHolders = all_tickers_majorHolders.append(ticker_majorHolders)
        all_tickers_majorHolders = all_tickers_majorHolders.reset_index(drop=True)
        return all_tickers_majorHolders
        
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
        all_info = pd.DataFrame()

        for ticker in tickers:
            if ticker.info is None:
                all_info[ticker.ticker] = np.nan
            else:
                stockInfo = pd.DataFrame(ticker.info.values(), index = ticker.info.keys(), columns= [ticker.ticker])
                all_info = pd.concat([all_info, stockInfo])
        return all_info
        
    def getOptionsExpirations(self,tickers):
        # Get options expirations
        options_dict = {}

        for ticker in tickers:
            options_dict[ticker.ticker] = list(ticker.options)
            df_options = pd.DataFrame.from_dict(options_dict, orient = 'index').transpose()
        return df_options
    
    def getSustainability(self, tickers):
        # Get Sustainability

        all_sustainability = pd.DataFrame()

        for ticker in tickers:
            if ticker.sustainability is None:
                all_sustainability[ticker.ticker] = np.nan
            else:
                sustainability = ticker.sustainability.rename(columns={"Value": ticker.ticker})
                all_sustainability = pd.concat([all_sustainability, sustainability])
        return all_sustainability
        
    def getCalendar(self,tickers):
        # Get next event (earnings, etc)
        all_calendar = pd.DataFrame()
        for ticker in tickers:
            if ticker.calendar is None:
                data= {'Earnings Date': np.nan,'Earnings Average': np.nan,'Earnings Low':np.nan,'Earnings High':np.nan,'Revenue Average':np.nan,
                                    'Revenue Low':np.nan,    'Revenue High':np.nan,    'Ticker':ticker.ticker}
                ticker_calendar = pd.DataFrame(data, index=['Value'])
                calendar = pd.concat([all_calendar,ticker_calendar])

            else:
                ticker_calendar = ticker.calendar.transpose()
                ticker_calendar['Ticker'] = ticker.ticker
                all_calendar = pd.concat([all_calendar,ticker_calendar])
            return all_calendar
    
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
            listed, delisted = self.checkTickers(companyCode)
            dict['Listed'] = listed
            dict['Delisted'] = delisted

            # Getting listed tickers history data
            dict['Historical Data'] = self.getHistoricalData(listed)

            tickers = [yf.Ticker(ticker) for ticker in listed]
            # Getting Financial Statement
            dict['Financial Statement'] = self.getFinancialStatement(tickers)

            # Getting Quarterly Financial Statement
            dict['Quarterly Financial Statement'] = self.getQuarterlyFinancialStatement(tickers)

            # Getting ISIN code (International Securities Identification Number)
            dict['isin'] = self.getISINcode(tickers)

            #Getting tickers revenues and earnings
            revenues, earnings = self.getEarningsandRevenue(tickers)
            dict['revenues'] = revenues
            dict['earnings'] = earnings

            #Getting tickers quarterly revenues and earnings
            qrevenues, qearnings = self.getQuarterlyEarningsandRevenue(tickers)
            dict['quarterly revenues'] = qrevenues
            dict['quarterly earnings'] = qearnings

            # Getting tickers major holders
            dict['Major Holders'] = self.getMajorHolders(tickers)
            
            # Getting tickers basic shares
            dict['Basic Shares'] =  self.getBasicShares(tickers)

            # Getting tickers stock information
            dict['Stock Information'] = self.getStockInfo(tickers)

            # Getting Tickers options expirations
            dict['Options Expiration'] = self.getOptionsExpirations(tickers)

            # Getting tickers sustainability
            dict['sustainability'] = self.getSustainability(tickers)

            # Getting tickers calendar - next events
            dict['calendar'] = self.getCalendar(tickers)

            # Getting tickers recommendations
            dict['recommendations'] = self.getRecommendations(tickers)

            # Getting tickers analysis
            dict['analysis'] = self.getAnalysis(tickers)

            # Getting tickers news
            # NOT WORKING - AM FIXING IT
            dict['news'] = self.getNews(tickers)

            # Getting tickers Mutual Fund Holders
            dict['Mutal Fund Holders'] = self.getMutualFundHolders(tickers)
            
            # Getting tickers Institutional Holders
            dict['Institutional Holders'] = self.getInstitutionalHolders(tickers)
            return dict
def main():
main()
    
    
