from cgitb import text
import pandas as pd
import re
import config


class TickerExtractor:
    def __init__(self):
        print("Initialising Ticker Extractor...")
        self.text_series = None
        print("Querying SGX Data...")
        # Will need to replace with GBQ database query
        self.SGX_data = pd.read_csv("SGX_data.csv")
        print("Successfully retrieved SGX Data")
        print("Initialising Mappers...")
        self.SGX_ticker_map = {x: y for x, y in zip(
            self.SGX_data["company_name"], self.SGX_data["company_code"])}

        # Handling individual special case stocks

        extended_dict = dict()

        for company_name, company_code in self.SGX_ticker_map.items():
            for map_in, map_out in config.word_mapper.items():
                if map_in in company_name:
                    new_name = company_name.replace(map_in, map_out)
                    extended_dict[new_name] = company_code
            # Handling stocks in camel casing
            company_name_split = " ".join(re.findall(
                r'[A-Z0-9](?:[a-z&]*|[A-Z]*(?=[A-Z]|$)*)', company_name))
            if "  " not in company_name_split and len(company_name_split) > 1:
                extended_dict[company_name_split] = company_code
        self.SGX_ticker_map = {**self.SGX_ticker_map, **extended_dict}
        print("Successfully Initialised Mappers")

    def load_text_series(self, text_series):
        if isinstance(text_series, pd.core.series.Series):
            self.text_series = text_series
        else:
            raise TypeError("Input is not Series of String type!")

    def extract_ticker_from_text(self, text):
        company_code_container = dict()  # list()
        text = str(text)
        for company_name, company_code in self.SGX_ticker_map.items():
            regexp_searcher_name = re.compile(
                re.escape(company_name))
            regexp_searcher_code = re.compile(
                re.escape(company_code))
            if regexp_searcher_name.search(text) or regexp_searcher_code.search(text):
                #print(f"Match found for {company_name} {company_code}")
                company_code_container[company_code] = company_name
        return company_code_container

    def extract_tickers_from_text_series(self):
        print("Extracting tickers from text...")
        tickers_found = self.text_series.apply(
            self.extract_ticker_from_text)
        self.results_df = pd.DataFrame(
            {"Text": self.text_series, "Tickers_found": tickers_found})

    def populate_ticker_occurences(self, text_series):
        self.load_text_series(text_series)
        self.extract_tickers_from_text_series()
        print("Tickers successfully extracted and populated")
        return self.results_df
