from cgitb import text
import pandas as pd
import re


class TickerExtractor:
    def __init__(self):
        self.text_series = None
        # Will need to replace with GBQ database query
        self.SGX_data = pd.read_csv("SGX_data.csv")
        self.SGX_ticker_map = {x: y for x, y in zip(
            self.SGX_data["company_name"], self.SGX_data["company_code"])}

        self.word_mapper = {"Intl": "Int", "intl": "int",
                            "YZJ Shipbldg SGD": "Yangzijiang Shipbuilding"}

        extended_dict = dict()

        for company_name, company_code in self.SGX_ticker_map.items():
            for map_in, map_out in self.word_mapper.items():
                if map_in in company_name:
                    new_name = company_name.replace(map_in, map_out)
                    print(new_name)
                    extended_dict[new_name] = company_code
        print(extended_dict)

        self.SGX_ticker_map = {**self.SGX_ticker_map, **extended_dict}
        # print(self.SGX_ticker_map)

    def load_text_series(self, text_series):
        if isinstance(text_series, pd.core.series.Series):
            self.text_series = text_series
        else:
            raise TypeError("Input is not Series of String type!")

    def extract_ticker_from_text(self, text):
        company_code_container = dict()  # list()
        text = str(text)
        for company_name, company_code in self.SGX_ticker_map.items():
            #print(company_name, company_code)
            regexp_searcher_name = re.compile(
                re.escape(company_name), re.IGNORECASE)
            regexp_searcher_code = re.compile(
                re.escape(company_code))
            #print(re.escape(company_code), re.escape(company_name))
            if regexp_searcher_name.search(text) or regexp_searcher_code.search(text):
                #print(f"Match found for {company_name} {company_code}")
                company_code_container[company_code] = company_name
        return company_code_container

    def extract_tickers_from_text_series(self):
        tickers_found = self.text_series.apply(
            self.extract_ticker_from_text)
        self.results_df = pd.DataFrame(
            {"Text": self.text_series, "Tickers_found": tickers_found})

    def populate_ticker_occurences(self, text_series):
        self.load_text_series(text_series)
        self.extract_tickers_from_text_series()
        return self.results_df
