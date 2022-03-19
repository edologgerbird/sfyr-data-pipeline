import pandas as pd
import re
import json


class TickerExtractor:
    def __init__(self):
        print("Initialising Ticker Extractor...")
        with open('utils/serviceAccount.json', 'r') as jsonFile:
            self.config = json.load(jsonFile)["tickerExtractor"]
        self.text_series = None
        self.text_series_reduced = None
        print("Querying SGX Data...")
        # Will need to replace with GBQ database query
        self.SGX_data = pd.read_csv("csv_store/SGX_data.csv")
        print("Successfully retrieved SGX Data")
        print("Initialising Mappers...")
        self.SGX_ticker_map_clean = {x: y for x, y in zip(
            self.SGX_data["company_code"], self.SGX_data["company_name"])}
        self.SGX_ticker_map = {x: set([y]) for x, y in zip(
            self.SGX_data["company_code"], self.SGX_data["company_name"])}
        for company_code, company_name_list in self.SGX_ticker_map.items():
            for company_name in company_name_list:
                new_name_cont = set()
                # Handles Camel Casing
                company_name_camel_split = " ".join(re.findall(
                    r'[A-Z0-9](?:[a-z]*|[A-Z]*(?=[A-Z]|$)*)', company_name))
                if len(company_name_camel_split) > 1 and self.check_single_tokens(company_name_camel_split):
                    new_name_cont.add(company_name_camel_split)
                # Handles special mapping cases
                for map_in, map_out in self.config["word_mapper"].items():
                    if map_in in company_name:
                        new_name = company_name.replace(map_in, map_out)
                        new_name_cont.add(new_name)
                        if len(self.remove_last_word(new_name)) > 1 and self.remove_last_word(new_name) not in self.config["exclusion"]:
                            new_name_cont.add(self.remove_last_word(new_name))
                if len(self.remove_last_word(company_name)) > 1 and self.remove_last_word(company_name) not in self.config["exclusion"]:
                    new_name_cont.add(self.remove_last_word(company_name))

            company_name_list = company_name_list | new_name_cont
            self.SGX_ticker_map[company_code] = list(company_name_list)

    def remove_last_word(self, text):
        text_split = text.split()
        return " ".join(text_split[:-1]) if len(text_split) > 1 else text

    def check_single_tokens(self, text):
        text_split = text.split()
        for i in text_split:
            if len(i) == 1:
                return False
        return True

    def return_title_case_only(self, text):
        text = str(text).replace(".", " ")
        output = ""
        for word in text.split():
            if not word.islower():
                output += (word + " ")
        return output

    def load_text_series(self, text_series):
        if isinstance(text_series, pd.core.series.Series):
            self.text_series = text_series
            self.text_series_reduced = text_series.apply(
                self.return_title_case_only)
        else:
            raise TypeError("Input is not Series of String type!")

    def extract_ticker_from_text(self, text):
        company_code_container = dict()
        text = str(text)
        for company_code, company_name_container in self.SGX_ticker_map.items():
            regexp_searcher_code = re.compile(re.escape(company_code))
            if regexp_searcher_code.search(text):
                company_code_container[company_code] = self.SGX_ticker_map_clean[company_code]
                continue
            for company_name in company_name_container:
                regexp_searcher_name = re.compile(re.escape(company_name))
                if regexp_searcher_name.search(text):
                    company_code_container[company_code] = self.SGX_ticker_map_clean[company_code]
                    break
        return company_code_container

    def extract_tickers_from_text_series(self):
        print("Extracting tickers from text...")
        tickers_found = self.text_series_reduced.apply(
            self.extract_ticker_from_text)
        self.results_df = pd.DataFrame(
            {"Text": self.text_series, "Tickers_found": tickers_found})

    def populate_ticker_occurences(self, text_series):
        self.load_text_series(text_series)
        self.extract_tickers_from_text_series()
        print("Tickers successfully extracted and populated")
        return self.results_df
