from operator import index
import pandas as pd
from urllib.request import Request, urlopen
import json


class SGXDataExtractor:
    def __init__(self):
        self.url = "https://api.sgx.com/securities/v1.1?params=nc%2Cn%2Ctype%2Cls%2Cm%2Csc%2Cbl%2Csip%2Cex%2Cej%2Cclo%2Ccr%2Ccur%2Cel%2Cr%2Ci%2Ccc%2Cig%2Clf"

    def extract_SGX_json_data(self):
        self.url_request = self.url_request = Request(
            self.url, headers={'User-Agent': 'Mozilla/5.0'})
        self.json_data = json.loads(urlopen(self.url_request).read())[
            "data"]["prices"]

    def populate_SGX_data(self):
        SGX_data_store = {
            "company_name": [],
            "company_code": [],
            "trading_time": [],
            "type": [],
            "listing_board": []
        }
        for company in self.json_data:
            SGX_data_store["company_name"].append(company["n"])
            SGX_data_store["company_code"].append(company["nc"])
            SGX_data_store["trading_time"].append(company["trading_time"])
            SGX_data_store["type"].append(company["type"])
            SGX_data_store["listing_board"].append(company["m"])

        self.SGX_data_store = pd.DataFrame(SGX_data_store).dropna()
        print(self.SGX_data_store)

    def SGX_data_to_csv(self):
        self.SGX_data_store.to_csv("SGX_data.csv", index=False)

    def get_SGX_data(self):
        self.extract_SGX_json_data()
        self.populate_SGX_data()
        self.SGX_data_to_csv()
