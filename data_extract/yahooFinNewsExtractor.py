from yahoo_fin import news
from tqdm import tqdm
import pandas as pd
from data_load.bigQueryAPI import bigQueryDB


class yahooFinNewsExtractor:
    def __init__(self):
        self.datasetTable = "SGX.Tickers"
        print("INFO: yahooFinNewsExtractor Initialised")

    def getTickerNews(self, tickers, sgFlag=False):
        output = []
        print("INFO: Extracting Ticker News from Yahoo Fin")
        for i in tqdm(range(len(tickers))):
            ticker = tickers[i]
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
        gbqQueryOutput = bigQueryDB().getDataFields(self.datasetTable, "ticker")
        sgxTickers = gbqQueryOutput.loc[:, "ticker"].tolist()
        return self.getTickerNews(sgxTickers, True)
