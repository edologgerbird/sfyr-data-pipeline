from datetime import datetime as dt
from time import mktime
import pandas as pd


class yahooFinNewsTransformer:
    def __init__(self):
        self.data_pending_upload = None
        self.articles = []

    def tickerNewsFormat(self, news, start_date=None, end_date=dt.now()):
        """This function formats news extracted from yahooFinNewExtractor into a format suitable for upload to Firestore. 
        This function also provides the ability to filter news articles by their publish date

        Args:
            news (list): List of all the news articles extracted
            start_date (datetime, optional): Earliest article publish date to be included. Defaults to None.
            end_date (datetime, optional): Latest article publish date to be included. Defaults to today.

        Returns:
            dataframe: Dataframe of the formatted news articles and filtered by publish date
        """
        newsFormatted = []
        articles = []

        # Start Date <= End Date Validation
        if (start_date is not None and end_date is not None and start_date > end_date):
            raise Exception(
                f'ERROR: Start date {start_date} input must be before end date {end_date} input')

        for i in range(0, len(news)):
            print(
                f">> ========== Formatting Yahoo-Fin News Overall Progress: {i}/{len(news)}")
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

        print(f"SUCCESS: yahooFinNews Transformed")
        self.data_pending_upload = newsFormatted

        pdArticles = pd.DataFrame(articles, columns=["message"])
        return pdArticles

    def finBERTFormat(self, sentiments):
        """Associate FinBERT sentiments output with each news article

        Args:
            sentiments (dataframe): Dataframe output from FinBERT

        Returns:
            dataframe: Dataframe of all news articles associated with sentiments
        """
        print(f"INFO: FinBERT Article Association Triggered")
        for i in range(0, len(self.data_pending_upload)):
            print(
                f">> ========== Sentiments Association Overall Progress: {i}/{len(sentiments)}")
            self.data_pending_upload[i]["sentiments"] = {
                "negative": sentiments.iloc[i]["Negative"],
                "neutral": sentiments.iloc[i]["Neutral"],
                "positive": sentiments.iloc[i]["Positive"],
            }
        print(f"SUCCESS: FinBERT Association Completed")
        return self.data_pending_upload
