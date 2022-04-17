'''
Generate Heatlists from Firestore Query Results
'''
from data_processing.HeatListGeneratorAPI import HeatListGenerator


class GenerateHeatlistsFromQuery:
    def __init__(self, sgx_data, industry_df):
        self.HeatListGenerator_layer = HeatListGenerator(sgx_data, industry_df)
        print("INFO: Heat List Generator Initialised")

    def generate_heatlist(self, query_results):
        print("INFO: Generating Heatlist from Queried Results from lookback period")
        ticker_heatlist, industry_heatlist = self.HeatListGenerator_layer.generateHeatList(
            query_results)
        print("SUCCESS: Ticker and Industry Heatlists generated")

        return ticker_heatlist, industry_heatlist

    def HeatlistPipeline_execute(self, query_results):
        self.ticker_heatlist, self.industry_heatlist = self.generate_heatlist(
            query_results)
        return self.ticker_heatlist, self.industry_heatlist
