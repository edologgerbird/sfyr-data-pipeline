from yahoo_fin import news
from tqdm import tqdm
import pandas as pd
from data_load.bigQueryAPI import bigQueryDB

class yahooFinNewsExtractor:
    def __init__(self):
        self.datasetTable = "SGX.Tickers"

    def getTickerNews(self, tickers, sgFlag=False):
        output = []
        print("Yahoo_fin: Extracting Ticker News")
        for i in tqdm(range(len(tickers))):
            ticker = tickers[i]
            if sgFlag:
                ticker = ticker + ".SI"
            tickerNews = news.get_yf_rss(ticker)
            if not tickerNews:
                output.append([ticker, tickerNews])
        output = pd.DataFrame(output)
        output.columns = ["Ticker", "News"]
        return output
    
    def getSGXTickerNews(self):
        gbqQueryOutput = bigQueryDB.getDataFields(self.datasetTable,"company_code")
        sgxTickers = gbqQueryOutput.loc[:,"company_code"].tolist()
        return self.getTickerNews(sgxTickers, True)

