from datetime import datetime as dt
from time import mktime
from utils.utils import splitter
import pandas as pd
from data_load.firestoreAPI import firestoreDB
from data_processing.FinBertAPI import FinBERT

class yahooFinNewsPipeline:
    def __init__(self):
        print("Initialising Firestore Pipeline...")
        self.firestoreDB_layer = firestoreDB()
        self.FinBERT_layer = FinBERT()
        self.data_pending_upload = None
        self.articles = []
        print("Firestore Pipeline Initialised")

    def tickerNewsFormat(self, news, start_date=None, end_date=dt.now()):
        newsFormatted = []
        articles = []
        
        # Start Date <= End Date Validation
        if (start_date is not None and end_date is not None and start_date > end_date):
            raise Exception('Start date input must be before end date input')

        for i in range(0,len(news)):
            ticker = news.at[i,"Ticker"]
            tickerNews = news.at[i,"News"]
            for article in tickerNews:
                articleFormatted = {
                    "date": dt.fromtimestamp(mktime(article["published_parsed"])),
                    "link": article["link"],
                    "title": article["title"],
                    "article": article["summary"],
                    "basequery": article["summary_detail"]["base"],
                    "tickers": [ticker]
                }
                
                if start_date != None:
                    if articleFormatted["date"] <= end_date and articleFormatted["date"] >= start_date:
                        newsFormatted.append(articleFormatted)
                        articles.append(article["summary"])
                else:
                    if articleFormatted["date"] <= end_date:
                        newsFormatted.append(articleFormatted)
                        articles.append(article["summary"])

        # NLP for Yahoo-Fin-News Data sentiments
        pdArticles = pd.DataFrame(articles)
        pdArticles.columns = ["message"]
        yahoo_fin_news_sentiments = self.FinBERT_layer.FinBert_pipeline(
            pdArticles["message"]
        )
        for i in range (0, len(newsFormatted)):
            newsFormatted[i]["sentiments"] = {
                "negative": yahoo_fin_news_sentiments.iloc[i]["Negative"],
                "neutral": yahoo_fin_news_sentiments.iloc[i]["Neutral"],
                "positive": yahoo_fin_news_sentiments.iloc[i]["Positive"],
            }
        self.data_pending_upload = newsFormatted
        return self.data_pending_upload

    def newsToFirestore(self):
        self.firestoreDB_layer.fsAddListofDocuments("Yahoo-Fin-News", self.data_pending_upload)
        self.data_pending_upload = None
        print("Yahoo News Data Uploaded")
    
