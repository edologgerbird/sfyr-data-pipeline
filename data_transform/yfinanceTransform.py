import pandas as pd


class yfinanceTransform:
    def __init__(self, yfinance_data):
        self.yfinance_data = yfinance_data

        for datafield in self.yfinance_data.keys():
            self.replaceColumnName(datafield)
            self.removeDuplicateColumns(datafield)
            print(f"SUCCESS: Transformation of {datafield} Complete")

        print("SUCCESS: yFinance Transform Completed")
        return self.yfinance_data

    def replaceColumnName(self, datafield):
        # Removing Spaces in Column Names - GBQ Limitation
        print(f"INFO: {datafield} Column Name Replacement Triggered")

        print("INFO: Step 1 - Removing Spaces")
        # Removing Spaces in Column Names - GBQ Limitation
        self.yfinance_data[datafield].columns = self.yfinance_data[datafield].columns.str.replace(
            ' ', '_')

        print("INFO: Step 2 - Renaming Column Name Start")
        # Add "_" to start if Column Names start with a number - GBQ Limitation
        # Replace "%" with Percentage - GBQ Limitation
        yfinanace_data_columns = self.yfinance_data[datafield].columns.tolist(
        )
        yfinance_formatted_columns = {}
        for columnName in yfinanace_data_columns:
            if columnName[0].isdigit():
                newName = "_" + columnName
                yfinance_formatted_columns[columnName] = newName
            elif "%" in columnName:
                newName = columnName.replace('%', 'percentage')
                yfinance_formatted_columns[columnName] = newName
            else:
                yfinance_formatted_columns[columnName] = columnName

        self.yfinance_data[datafield].rename(
            columns=yfinance_formatted_columns, inplace=True)

        print(f"SUCCESS: {datafield} Column Name Replacement")

    def removeDuplicateColumns(self, datafield):
        print(f"INFO: Removing Column Duplicates - {datafield}")
        self.yfinance_data[datafield] = self.yfinance_data[datafield].loc[:,
                                                                          ~self.yfinance_data[datafield].columns.duplicated()]
        print(f"SUCCESS: {datafield} Column Duplicates Removed")
