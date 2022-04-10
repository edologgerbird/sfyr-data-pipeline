import pandas as pd
from urllib.request import Request, urlopen
import json
from data_load.bigQueryAPI import bigQueryDB

class SGXDataExtractor:
    def __init__(self):
        with open('utils/serviceAccount.json', 'r') as jsonFile:
            self.cred = json.load(jsonFile)
        self.url = self.cred["dataSources"]["sgx_api"]
        self.url_request = None
        self.json_data = None
        self.SGX_data_store = None
        self.updated_SGX_data_store = None

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
        self.SGX_data_store['active'] = True
        print("SGX Data successfully populated")

    def get_SGXData_from_GBQ(self):
        if bigQueryDB().gbqCheckTableExist("SGX.Tickers"):
            return bigQueryDB().getDataFields("SGX.Tickers")
        else:
            return None

    # Checks scrapped data with ticker data from GBQ to update their 'active' status
    def update_ticker_status(self):
        # 3 Cases to consider
        # Case 1 - Previously delisted tickers
        # Case 2 - Newly delisted tickers 
        # Case 3 - Previously delisted tickers that are now active
 
        #Getting active SGX tickers dataframe
        active_SGX_ticker_df = self.SGX_data_store 
        active_SGX_ticker_list = active_SGX_ticker_df.company_code.to_list()
        
        #Getting SGX tickers dataframe from GBQ
        GBQ_SGX_ticker_df = self.get_SGXData_from_GBQ() 

        if GBQ_SGX_ticker_df is None:
            self.updated_SGX_data_store = self.SGX_data_store
            return self.updated_SGX_data_store

        #Getting the list/dataframe of tickers from GBQ (Active and delisted)
        active_GBQ_ticker_df = GBQ_SGX_ticker_df[GBQ_SGX_ticker_df['active'] == True]
        delisted_GBQ_ticker_df = GBQ_SGX_ticker_df[GBQ_SGX_ticker_df['active'] == False]
        
        active_GBQ_ticker_list = active_GBQ_ticker_df.company_code.to_list()

        # This list contains either active tickers from SGX only or active tickers from GBQ only
        active_SGX_or_active_GBQ_list = list(set(active_SGX_ticker_list) ^ set(active_GBQ_ticker_list))

        #Merges all tickers from SGX data store (all active) and delisted GBQ dataframe (contains previously delisted tickers - case 1 is considered)
        #By dropping duplicates and keeping only the active one, case 3 is considered
        unique_columns = ["company_name", "company_code", "trading_time", "type", "listing_board"]
        updated_SGX_data = pd.concat([active_SGX_ticker_df, delisted_GBQ_ticker_df]).drop_duplicates(subset = unique_columns, keep = 'first')
        
        for ticker in active_SGX_or_active_GBQ_list:
            if ticker not in active_SGX_ticker_list:  #Changing status for newly delisted tickers - case 2 is considered
                newly_delisted_entry = active_GBQ_ticker_df.loc[active_GBQ_ticker_df['company_code'] == ticker].copy()
                newly_delisted_entry['active'] = False
                updated_SGX_data = pd.concat([updated_SGX_data, newly_delisted_entry])
        
        self.updated_SGX_data_store = updated_SGX_data.sort_values(['company_name', 'company_code']).reset_index(drop=True)

        return self.updated_SGX_data_store

    def SGX_data_to_csv(self):
        self.updated_SGX_data_store.to_csv("SGX_data.csv", index=False)
        print("SGX Data successfully saved to CSV")

    def SGX_data_to_bg(self):
        if bigQueryDB().gbqCheckTableExist("SGX.Tickers"):
            return bigQueryDB().gbqReplace(self.updated_SGX_data_store, "SGX.Tickers")
        else: 
            return bigQueryDB().gbqCreateNewTable(self.updated_SGX_data_store, "SGX", "Tickers")

    def load_SGX_data_from_source(self):
        self.extract_SGX_json_data()
        self.populate_SGX_data()
        self.update_ticker_status()
        self.SGX_data_to_bg()
        # self.SGX_data_to_csv() # Depreciated 

    def get_SGX_data(self):
        self.extract_SGX_json_data()
        self.populate_SGX_data()
        return self.SGX_data_store