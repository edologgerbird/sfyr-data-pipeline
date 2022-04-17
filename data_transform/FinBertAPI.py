from matplotlib.pyplot import text
import pandas as pd
import numpy as np
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch
import gc


class FinBERT:
    def __init__(self):
        print("INFO: Initialising FinBERT model")
        self.tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
        self.model = AutoModelForSequenceClassification.from_pretrained(
            "ProsusAI/finbert")
        self.batch_size = 1
        print("INFO: FinBERT model initialised")

    def load_text_data(self, text_series):
        """This function takes in a Pandas Series of String data, and stores it as a class variable

        Args:
            text_series (pandas.Series): A Series of String

        Raises:
            Exception: Exception when input type is not Series

        Returns:
            list: Returns a list of String chunks
        """
        if not isinstance(text_series, pd.core.series.Series):
            raise Exception("ERROR: Input text not in Series!")
        else:
            print("INFO: Loading text data")
            text_array = np.array(text_series)
            text_list = list(text_array)
            print("SUCCESS: Text data loaded")
            return text_list

    def tokenize_text(self, text_list):
        """Tokenizes a list of String to prepare for FinBERT model analysis.

        Args:
            text_list (list): a list of Strings

        Returns:
            transformers.tokenization_utils_base.BatchEncoding: tensors of tokenized text
        """
        print("INFO: Tokenizing Text")
        return self.tokenizer(text_list, padding=True, truncation=True, return_tensors='pt')

    def predict_sentiments(self, text_list, inputs):
        """Predicts the sentiments of a given input of tokens.

        Args:
            text_list (list): list of text chunks
            inputs (transformers.tokenization_utils_base.BatchEncoding): input tensors of tokens

        Returns:
            pd.DataFrame: returns a DataFrame of text along with the corresponding sentiment scores (Positive, Negative, Neutral)
        """
        print("INFO: Predicting sentiments")
        outputs = self.model(**inputs)
        predictions = torch.nn.functional.softmax(outputs.logits, dim=-1)
        print("SUCCESS: Sentiments successfully predicted")
        positive = predictions[:, 0].tolist()
        negative = predictions[:, 1].tolist()
        neutral = predictions[:, 2].tolist()
        table = {'Text': text_list,
                 "Positive": positive,
                 "Negative": negative,
                 "Neutral": neutral}

        df = pd.DataFrame(
            table, columns=["Text", "Positive", "Negative", "Neutral"])

        return df

    def FinBert_pipeline(self, text_series):
        """Executes the FinBert analysis pipeline given an input Series of text. The chunk size can be modified based on system memory capacity.

        Args:
            text_series (pd.Series): input Series of text to be passed in FinBERT sentiment analysis

        Returns:
            pd.DataFrame: returns a DataFrame of text along with the corresponding sentiment scores
        """
        predictions_mega = pd.DataFrame(
            columns=["Text", "Positive", "Negative", "Neutral"])
        if len(text_series) == 0:
            return predictions_mega
        if len(text_series) < self.batch_size:
            self.batch_size = max(len(text_series), 1)
        chunks = np.array_split(text_series, len(text_series)/self.batch_size)
        chunk_counter = 1
        total_chunks = len(chunks)
        for chunk in chunks:
            print(
                f"INFO: Performing FinBERT Analysis on Chunk {chunk_counter} / {total_chunks}")
            text_list = self.load_text_data(chunk)
            if not text_list[0]:
                predictions = pd.DataFrame(
                    columns=["Text", "Positive", "Negative", "Neutral"])
                predictions.loc[len(predictions)] = 0
            else:
                tokenized = self.tokenize_text(text_list)
                predictions = self.predict_sentiments(text_list, tokenized)

            predictions_mega = pd.concat([predictions_mega, predictions])
            gc.collect()
            chunk_counter += 1

        print("SUCCESS: FinBERT analysis completed")
        return predictions_mega
