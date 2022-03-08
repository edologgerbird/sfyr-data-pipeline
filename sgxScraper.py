from operator import index
import pandas as pd
from urllib.request import Request, urlopen
import json


class SGXStocksExtractor:
    def __init__(self):
        self.url_prefix = "https://api.sgx.com/securities/v1.1/gtis?start="
        self.url_suffix = "&size=905"
        self.url_request = Request(
            f"{self.url_prefix}1{self.url_suffix}", headers={'User-Agent': 'Mozilla/5.0'})
        page_one_json = json.loads(urlopen(self.url_request).read())
        self.total_pages = page_one_json["meta"]["totalPages"]

    def extractSGXStocks(self):
        SGX_stocks_dict = {"company_name": [], "stock_code": []}
        for page in range(1, self.total_pages+1):
            current_page_request = Request(f"{self.url_prefix}{str(page)}{self.url_suffix}", headers={
                                           'User-Agent': 'Mozilla/5.0'})
            current_json = json.loads(urlopen(current_page_request).read())
            if current_json["data"]:
                for stock in current_json["data"]:
                    current_company_name = stock["companyName"]
                    current_company_code = stock["stockCode"]
                    SGX_stocks_dict["company_name"].append(
                        current_company_name)
                    SGX_stocks_dict["stock_code"].append(
                        current_company_code)
        pd.DataFrame(SGX_stocks_dict).to_csv(
            "SGXstocks.csv", index=False)
        print("Stocks data exported")
        return SGX_stocks_dict
