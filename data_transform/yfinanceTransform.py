import pandas as pd
import json


class yfinanceTransform:
    def __init__(self, yfinance_data):
        self.yfinance_data = yfinance_data

        for dataset in self.yfinance_data.keys():
            self.replaceColumnName(dataset)
            self.removeDuplicateColumns(dataset)
            print(f"SUCCESS: Transformation of {dataset} Complete")

        print("SUCCESS: yFinance Transform Completed")
        return self.yfinance_data

    def replaceColumnName(self, dataset):
        # Removing Spaces in Column Names - GBQ Limitation
        print(f"INFO: {dataset} Column Name Replacement Triggered")

        print("INFO: Step 1 - Removing Spaces")
        # Removing Spaces in Column Names - GBQ Limitation
        self.yfinance_data[dataset].columns = self.yfinance_data[dataset].columns.str.replace(
            ' ', '_')

        print("INFO: Step 2 - Renaming Column Name Start")
        # Add "_" to start if Column Names start with a number - GBQ Limitation
        # Replace "%" with Percentage - GBQ Limitation
        yfinanace_data_columns = self.yfinance_data[dataset].columns.tolist(
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

        self.yfinance_data[dataset].rename(
            columns=yfinance_formatted_columns, inplace=True)

        print(f"SUCCESS: {dataset} Column Name Replacement")

    def removeDuplicateColumns(self, dataset):
        print(f"INFO: Removing Column Duplicates - {dataset}")
        self.yfinance_data[dataset] = self.yfinance_data[dataset].loc[:,
                                                                      ~self.yfinance_data[dataset].columns.duplicated()]
        print(f"SUCCESS: {dataset} Column Duplicates Removed")

    def schemaCompliance(self, dataset):
        datatype_mapping = {
            "STRING": "object",
            "INTEGER": "int64",
            "FLOAT": "float64",
            'TIMESTAMP': 'datetime64',
            "BOOLEAN": "bool"
        }
        yfinance_dataset = self.yfinance_data[dataset]
        tableSchemaUrl = "utils/bigQuerySchema.json"
        with open(tableSchemaUrl, 'r') as schemaFile:
            tableSchema = json.load(schemaFile)
        dataset_schema = tableSchema[dataset]
        pd_dataset_schema = {}
        for dataset_column in dataset_schema:
            pd_dataset_schema[dataset_column["name"]
                              ] = datatype_mapping[dataset_column["type"]]
        yfinance_dataset.astype(pd_dataset_schema, copy=False)

        self.yfinance_data = yfinance_dataset
        return yfinance_dataset
