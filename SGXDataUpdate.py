from data_load.bigQueryAPI import bigQueryDB
from data_extract.SGXDataExtractor import SGXDataExtractor

class SGXDataUpdate:
    def __init__(self):
        self.GBQ_SGX_data = None
        self.scraped_SGX_data = None
        self.updated_SGX_data = None

    def get_scraped_SGX_data(self):
        SGX_data = SGXDataExtractor().get_SGX_data()
        SGX_data = SGX_data[["company_name", "company_code"]]
        self.scraped_SGX_data = SGX_data
        return self.scraped_SGX_data
        
    def get_SGXData_from_GBQ(self):
        if bigQueryDB().gbqCheckTableExist("SGX.Tickers"):
            self.GBQ_SGX_data = bigQueryDB().getDataFields("SGX.Tickers", "company_code", "company_name")
    
    def isActive(self, ticker, delisted_tickers_list):
        return ticker not in delisted_tickers_list

    def update_ticker_status(self):
        #Getting the list of scrapped SGX tickers 
        self.get_scraped_SGX_data()
        scrapped_ticker_list = self.scraped_SGX_data.company_code.to_list()

        #Getting the list of tickers from GBQ
        self.get_SGXData_from_GBQ()
        GBQ_ticker_list = self.GBQ_SGX_data.company_code.to_list()

        # If there is no SGX data in GBQ because running for first time, return all tickers
        # from scraped SGX data with "Active" under "status" column
        if self.GBQ_SGX_data is None:
            updated_SGX_data = self.scraped_SGX_data
            updated_SGX_data['status'] = "Active"
            return self.updated_SGX_data 

        # scrapped_ticker_list = ["1", "2", "3"]
        # GBQ_ticker_list = ["3", "4", "5"]
        newly_delisted_tickers_list = list(set(GBQ_ticker_list) - set(scrapped_ticker_list))

        GBQ_delisted_ticker_df = self.GBQ_SGX_data[self.GBQ_SGX_data.status == "False"]
        previously_delisted_tickers_list = GBQ_delisted_ticker_df.company_code.to_list()

        all_delisted_tickers_list = newly_delisted_tickers_list + previously_delisted_tickers_list

        updated_SGX_data = self.scraped_SGX_data.append(self.GBQ_SGX_data)
        updated_SGX_data = updated_SGX_data.drop_duplicates(subset="company_code", keep='first')
        updated_SGX_data['status'] = updated_SGX_data['company_code'].apply(lambda x: self.isActive(x['company_code'],all_delisted_tickers_list))
        
        self.updated_SGX_data = updated_SGX_data
        # GBQ_SGX_data = self.GBQ_SGX_data
        # delisted_ticker_df = GBQ_SGX_data[GBQ_SGX_data['company_code'].isin(all_delisted_tickers_list)]

        # active_ticker_df = self.scraped_SGX_data
        # active_ticker_df['status'] = "True"
        # self.updated_SGX_data = delisted_ticker_df.append(active_ticker_df, ignore_index=True)
        
        return self.updated_SGX_data
