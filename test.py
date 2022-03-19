import data_transform.TickerExtractor as TickerExtractor
import pandas as pd
import time
start_time = time.time()

ticker_extractor_layer = TickerExtractor.TickerExtractor()
data_in = pd.read_csv("csv_store/sbr_articles_stocks.csv").head(10)
df = ticker_extractor_layer.populate_ticker_occurences(data_in["Text"])
df.to_csv("csv_store/tickers_found_test.csv")

print("--- %s seconds ---" % (time.time() - start_time))
