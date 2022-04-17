import pandas as pd
import re


class STIMovementExtractor:
    def __init__(self):
        self.text_series = None
        self.direction_series = None
        self.percentage_series = None
        self.results_df = None
        print("INFO: Initialised STI Movement Extractor")

    def sti_related(self, title):
        """Helper Function extracts titles with STI Movements

        Args:
            title (string): Title to be checked

        Returns:
            boolean: True/False the item exist
        """
        title = title.lower()
        sti_lst = ['sti', 'straits times index']
        return any(title.find(word) > -1 for word in sti_lst)

    def direction_of_STI_movement(self, title):
        """This function extracts the direction of STI Movement

        Args:
            title (string): Title to be checked

        Returns:
            string: Negative or Postiive Movements
        """
        title = title.lower()
        positive_lst = ['up', 'gains', 'gain', 'higher']
        negative_lst = ['down', 'dips', 'dip', 'lose',
                        'loses', 'loss', 'lower', 'drop', 'drops']
        if self.sti_related(title):
            if any(title.find(word) > -1 for word in negative_lst):
                return 'Negative'
            elif any(title.find(word) > -1 for word in positive_lst):
                return 'Positive'
        else:
            return None

    def percentage_of_STI_movement(self, title):
        """This function extracts the percentage of STI Movement

        Args:
            title (string): Title to be checked

        Returns:
            string: STI movement percentage
        """
        if self.sti_related(title):
            try:
                result = re.search(r'((?:\d+%)|(?:\d+\.\d+%))', title).group()
            except AttributeError:
                result = re.search(r'((?:\d+%)|(?:\d+\.\d+%))', title)
            return result

    def populate_sti_movement(self, text_series):
        """This function populates the STI Movements into a dataframe

        Args:
            text_series (dataframe): Dataframe of text to be checked

        Returns:
            dataframe: Dataframe of Title, STI movement Direction and percentage
        """
        print("INFO: Extracting and populating STI movement direction and percentage")
        self.text_series = text_series
        self.direction_series = text_series.apply(
            self.direction_of_STI_movement)
        self.percentage_series = text_series.apply(
            self.percentage_of_STI_movement)
        print("SUCCESS: STI movement direction and percentage successfully extracted and populated")
        self.results_df = pd.DataFrame(
            {"Title": self.text_series, "Direction of STI Movement": self.direction_series, "Percentage of STI Movement": self.percentage_series})
        return self.results_df
