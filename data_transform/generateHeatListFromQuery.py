from data_transform.HeatListGeneratorAPI import HeatListGenerator


class GenerateHeatlistsFromQuery:
    def __init__(self, sgx_data, industry_df):
        """Initialises a Heat List generator for Query results

        Args:
            sgx_data (pd.DataFrame): a pd.DataFrame of SGX stocks details
            industry_df (pd.DataFrame): a pd.DataFrame of companies and their industries
        """
        self.HeatListGenerator_layer = HeatListGenerator(sgx_data, industry_df)
        print("INFO: Heat List Generator Initialised")

    def generate_heatlist(self, query_results):
        """Generates heatlist from query results

        Args:
            query_results (list): a list of String query results

        Returns:
            tuple: a tuple of 2 pd.DataFrames which are the TIcker heatlist and Industry heatlist
        """
        print("INFO: Generating Heatlist from Queried Results from lookback period")
        ticker_heatlist, industry_heatlist = self.HeatListGenerator_layer.generateHeatList(
            query_results)
        print("SUCCESS: Ticker and Industry Heatlists generated")

        return ticker_heatlist, industry_heatlist

    def HeatlistPipeline_execute(self, query_results):
        """Executes the heatlist pipeline given input query results and stores them as Class variables

        Args:
            query_results (list): a list of String query results

        Returns:
            tuple: a tuple of 2 pd.DataFrames which are the TIcker heatlist and Industry heatlist 
        """
        self.ticker_heatlist, self.industry_heatlist = self.generate_heatlist(
            query_results)
        return self.ticker_heatlist, self.industry_heatlist
