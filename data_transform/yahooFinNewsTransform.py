from datetime import datetime as dt
from time import mktime
import pandas as pd
import tqdm


class yahooFinNewsTransformer:
    def __init__(self):
        self.data_pending_upload = None
        self.articles = []

    def tickerNewsFormat(self, news, start_date=None, end_date=dt.now()):
        newsFormatted = []
        articles = []

        # Start Date <= End Date Validation
        if (start_date is not None and end_date is not None and start_date > end_date):
            raise Exception(f"ERROR: {start_date} is not before {end_date}")

        for i in tqdm(range(0, len(news))):
            ticker = news.at[i, "Ticker"]
            tickerNews = news.at[i, "News"]
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

        print(f"SUCCESS: Yahoo-Fin News Transformed")
        self.data_pending_upload = newsFormatted

        pdArticles = pd.DataFrame(articles, columns=["message"])
        print(f"SUCCESS: Generated Dataframe for FinBERT")
        return pdArticles

    def finBERTFormat(self, sentiments):
        print("INFO: Linking FinBERT to Articles")
        for i in tqdm(range(0, len(self.data_pending_upload))):
            self.data_pending_upload[i]["sentiments"] = {
                "negative": sentiments.iloc[i]["Negative"],
                "neutral": sentiments.iloc[i]["Neutral"],
                "positive": sentiments.iloc[i]["Positive"],
            }
        print(f"SUCCESS: FinBERT Linking Completed")
        return self.data_pending_upload
