'''
SBR Data Transformation
'''

from utils.utils import splitter

class SBRDataTransformer:
    def __init__(self):
        self.SBR_data_to_upload = None
        print("SBRDataTransformer Initialised")

    def transformSBRData(self, SBR_data_raw, SBR_data_with_tickers, SBR_data_with_sentiments):
        # # Combining Dataframes
        SBR_data_with_tickers["sentiment"] = [{"sentiment": {
            "positive": x, "negative": y, "neutral": z}}for x, y, z in zip(
            *splitter(SBR_data_with_sentiments[["Positive", "Negative", "Neutral"]])
        )]

        # self.SBR_data_processed = SBR_data_with_tickers
        SBR_data_processed = SBR_data_with_tickers
        SBR_data_processed[["Date", "Title", "Text", "Link"]
                                ] = SBR_data_raw[["Date", "Title", "Text", "Link"]]

        # Transforming Data to NoSQL format

        # SBR Data
        self.SBR_data_to_upload = [
            {"text_headline": headline,
             "text_body": body,
             "link": link,
             "date": date,
             "tickers": [ticker for ticker in tickers.keys()],
             "sti_movement": {"direction": direction, "amount": amount},
             "sentiments": list(sentiments.values())[0]}
            for headline, body, link, date, tickers, direction, amount, sentiments in zip(
                *splitter(SBR_data_processed[[
                    "Title", "Text", "Link", "Date", "Tickers_found", "STI_direction", "STI_movement", "sentiment"
                ]])
            )
        ]

        return self.SBR_data_to_upload