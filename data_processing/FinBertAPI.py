import pandas as pd
import numpy as np
import tokenizers
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch

class FinBERT:
    def __init__(self):
        print("Initialising FinBERT model...")
        self.tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
        self.model = AutoModelForSequenceClassification.from_pretrained("ProsusAI/finbert")
        print("FinBERT model initialised")

    def load_text_data(self, text_series):
        if not isinstance(text_series, pd.core.series.Series):
            raise Exception("Input text not in Series!")
        else:
            print("Loading text data...")
            text_array = np.array(text_series)
            np.random.shuffle(text_array)
            text_list = list(text_array)
            print("Text data loaded.")
            return text_list

    def tokenize_text(self, text_list):
        print("Tokenizing Text...")
        return self.tokenizer(text_list, padding=True, truncation=True, return_tensors='pt')

    def predict_sentiments(self, text_list, inputs):
        print("Predicting sentiments...")
        outputs = self.model(**inputs)
        predictions = torch.nn.functional.softmax(outputs.logits, dim=-1)
        print("Sentiments successfully predicted!")
        positive = predictions[:, 0].tolist()
        negative = predictions[:, 1].tolist()
        neutral = predictions[:, 2].tolist()
        table = {'Text':text_list,
         "Positive":positive,
         "Negative":negative, 
         "Neutral":neutral}
      
        df = pd.DataFrame(table, columns = ["Text", "Positive", "Negative", "Neutral"])
        print("Prediction exported as CSV")
        return df

    def FinBert_pipeline(self, text_series):
        text_list = self.load_text_data(text_series)
        tokenized = self.tokenize_text(text_list)
        predictions = self.predict_sentiments(text_list, tokenized)
        predictions.to_csv("predictions.csv")
        return predictions
        


## Test

# data = pd.read_csv("csv_store/sbr_articles_stocks.csv").head(15)
# data["Title_Text"] = data["Title"] + " " + data["Text"]
# FinBERT_layer = FinBERT()
# FinBERT_layer.FinBert_pipeline(data["Title_Text"])
