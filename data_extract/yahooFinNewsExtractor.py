from yahoo_fin import news
import pandas as pd
from data_load.bigQueryAPI import bigQueryDB


class yahooFinNewsExtractor:
    def __init__(self):
        self.datasetTable = "SGX.Tickers"
        print("INFO: yahooFinNewsExtractor Initialised")

    def getTickerNews(self, tickers, sgFlag=False):
        """This helper function calls the yahoo_fin News API and obtains news associated the tickers 

        Args:
            tickers (list): List of tickers to be used in extration
            sgFlag (bool, optional): True if the tickers to be extracted is in SGX which appends -SI suffix to tickers. Defaults to False.

        Returns:
            dataframe: Dataframe of ticker and associated news
        """
        output = []
        print("INFO: Extracting Ticker News from Yahoo Fin")
        for i in range(len(tickers)):
            ticker = tickers[i]
            print(
                f">> ========== Extracting {ticker} News Overall Progress: {i}/{len(tickers)}")
            tickerNews = None
            if sgFlag:
                tickerNews = news.get_yf_rss(ticker + ".SI")
            else:
                tickerNews = news.get_yf_rss(ticker)
            if tickerNews:
                output.append([ticker, tickerNews])
        output = pd.DataFrame(output, columns=["Ticker", "News"])
        print("SUCCESS: YahooFinNews successfully extracted")
        return output

    def getSGXTickerNews(self):
        """This function uses the getTickerNews function to generate news of all Tickers in SGX

        Returns:
            dataframe: Dataframe of ticker and associated news
        """
        gbqQueryOutput = bigQueryDB().getDataFields(self.datasetTable, "ticker")
        sgxTickers = gbqQueryOutput.loc[:, "ticker"].tolist()
        return self.getTickerNews(sgxTickers, True)
