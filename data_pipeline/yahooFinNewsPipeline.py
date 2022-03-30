import pandas as pd
from datetime import datetime
from time import mktime
from data_load.firestoreAPI import firestoreDB

class yahooFinNewsPipeline:
    def __init__(self):
        print("Initialising Firestore Pipeline...")
        self.firestoreDB_layer = firestoreDB()
        self.data_pending_upload = None
        print("Firestore Pipeline Initialised")

    def tickerNewsFormat(self, news):
        newsFormatted = []
        for i in range(0,len(news)):
            ticker = news.at[i,"Ticker"]
            tickerNews = news.at[i,"News"]
            for article in tickerNews:
                articleFormatted = {
                    "date": datetime.fromtimestamp(mktime(article["published_parsed"])),
                    "link": article["link"],
                    "title": article["title"],
                    "article": article["summary"],
                    "basequery": article["summary_detail"]["base"],
                    "ticker": ticker
                }
                newsFormatted.append(articleFormatted)
        self.data_pending_upload = newsFormatted
        return newsFormatted
    
    def newsToFirestore(self):
        self.firestoreDB_layer.fsAddListofDocuments("Yahoo-Fin-News", self.data_pending_upload)
        self.data_pending_upload = None
        print("Yahoo News Data Uploaded")
    
