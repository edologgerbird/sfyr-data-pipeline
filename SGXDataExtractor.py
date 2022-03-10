from operator import index
import pandas as pd
from urllib.request import Request, urlopen
import json
import config


class SGXDataExtractor:
    def __init__(self):
        self.url = config.sgx_api
        self.url_request = None
        self.json_data = None
        self.SGX_data_store = None

    def extract_SGX_json_data(self):
        print("Extracting SGX Data from API ...")
        self.url_request = self.url_request = Request(
            self.url, headers={'User-Agent': 'Mozilla/5.0'})
        self.json_data = json.loads(urlopen(self.url_request).read())[
            "data"]["prices"]
        print("SGX Data Successfully Extracted")

    def populate_SGX_data(self):
        print("Populating SGX Data Store ...")
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
        print("SGX Data successfully populated")

    def SGX_data_to_csv(self):
        self.SGX_data_store.to_csv("SGX_data.csv", index=False)
        print("SGX Data successfully saved to CSV")

    def load_SGX_data_from_source(self):
        self.extract_SGX_json_data()
        self.populate_SGX_data()
        self.SGX_data_to_csv()

    def get_SGX_data(self):
        return self.SGX_data_store