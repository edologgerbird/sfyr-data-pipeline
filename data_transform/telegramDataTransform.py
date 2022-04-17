from utils.utils import splitter


class telegramDataTransformer:
    def __init__(self):
        self.tele_data_to_upload = None
        print("INFO: telegramDataTransformer Initialised")

    def transformTelegramData(self, tele_data_raw, tele_data_with_tickers, tele_data_with_sentiments):
        """This function transforms Telegram Data into a single consolidated data

        Args:
            tele_data_raw (dataframe): Dataframe of Raw Tele Data 
            tele_data_with_tickers (dataframe):  Dataframe of Tele Data with Tickers 
            tele_data_with_sentiments (dataframe):  Dataframe of Tele Data with sentiments

        Returns:
            Dataframe: Transformed Tele Data for upload to Firestore
        """
        print("INFO: Transforming Teke Data")
        # # Combining Dataframes
        print("INFO: Combining Dataframes")
        tele_data_with_tickers["sentiment"] = [{"sentiment": {
            "positive": x, "negative": y, "neutral": z}} for x, y, z in zip(
            *splitter(tele_data_with_sentiments[["Positive", "Negative", "Neutral"]])
        )]
        print("SUCCESS: Dataframes Combined")

        print("INFO: Processing SBR Data with Tickers")
        # self.SBR_data_processed = SBR_data_with_tickers
        tele_data_processed = tele_data_with_tickers
        tele_data_processed[["channel", "date", "sender"]
                            ] = tele_data_raw[["channel", "date", "sender"]]
        print("SUCCESS: SBR Data with Tickers Processes")

        # Transforming Data to NoSQL format
        print("INFO: Transforming Data to NoSQL format")

        # Telegram Data
        self.tele_data_to_upload = [
            {"channel": channel,
             "date": date,
             "sender": sender,
             "message": message,
             "tickers": [ticker for ticker in tickers.keys()],
             "sentiments": list(sentiments.values())[0]}
            for channel, date, sender, message, tickers, sentiments in zip(
                *splitter(tele_data_processed[[
                    "channel", "date", "sender", "Text", "Tickers_found", "sentiment"
                ]])
            )
        ]
        print("SUCCESS: Telegram Data Transformed")

        return self.tele_data_to_upload
