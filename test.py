import TickerExtractor
import pandas as pd

ticker_extractor_layer = TickerExtractor.TickerExtractor()
data_in = pd.read_csv("sbr_articles_stocks.csv").head(15)
data_in_df = data_in["Text"]
df = ticker_extractor_layer.populate_ticker_occurences(data_in_df)
df.to_csv("tickers_found.csv")
