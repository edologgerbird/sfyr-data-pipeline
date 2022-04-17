import pandas as pd
import json
import traceback


class yfinanceTransform:
    def __init__(self, yfinance_data):
        self.yfinance_data = yfinance_data
        self.errors_all = dict()

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
        print(f"INFO: Removing Column Duplicates for {dataset}")
        self.yfinance_data[dataset] = self.yfinance_data[dataset].loc[:,
                                                                      ~self.yfinance_data[dataset].columns.duplicated()]
        print(f"SUCCESS: Duplicate Column Removed In {dataset} ")

    def schemaCompliance(self, dataset):
        errors = []

        print(f"INFO: {dataset} Schema Compliance Triggered")

        datatype_mapping = {
            "STRING": "string",
            "INTEGER": "Int64",
            "FLOAT": "Float64",
            'TIMESTAMP': 'datetime64',
            "BOOLEAN": "boolean"
        }
        tableSchemaUrl = "utils/bigQuerySchema.json"
        with open(tableSchemaUrl, 'r') as schemaFile:
            tableSchema = json.load(schemaFile)
        dataset_schema = tableSchema["yfinance." + dataset]

        pd_dataset_schema = {}
        yfinance_dataset = self.yfinance_data[dataset]
        for dataset_column in dataset_schema:
            pd_dataset_schema[dataset_column["name"]
                              ] = datatype_mapping[dataset_column["type"]]

        yfinance_dataset = yfinance_dataset.convert_dtypes()

        for col in yfinance_dataset.columns:

            try:
                if col not in pd_dataset_schema.keys():
                    print(f"INFO: Extra {col} Dropped")
                    yfinance_dataset.drop(labels=col, axis=1)
                elif yfinance_dataset[col].dtypes != pd_dataset_schema[col]:
                    print(
                        f"INFO: {col} Enforcing Schema {yfinance_dataset[col].dtype} -> {pd_dataset_schema[col]}")
                    if yfinance_dataset[col].dtype == "string" and pd_dataset_schema[col] == "Int64":
                        yfinance_dataset = yfinance_dataset.astype(
                            {col: "Float64"})
                        print(
                            f"INFO: Intermediate {yfinance_dataset[col].dtype} Enforcement for String -> Int ")

                    yfinance_dataset = yfinance_dataset.astype(
                        {col: pd_dataset_schema[col]})
                    print(
                        f"SUCCESS: {col} Datatype Enforced as {yfinance_dataset[col].dtype}")
            except:
                traceback_info = traceback.format_exc()
                print(traceback_info)
                errors.append([col, traceback_info])
                continue
        self.errors_all["dataset"] = errors
        print(f"SUCCESS: {dataset} Schema Compliance Enforced")
        self.yfinance_data[dataset] = yfinance_dataset
        return yfinance_dataset

    def transformData(self):
        for datafield in self.yfinance_data.keys():
            if not self.yfinance_data[datafield].empty:
                print(f"INFO: Transformation of {datafield} Triggered")
                self.replaceColumnName(datafield)
                self.removeDuplicateColumns(datafield)
                self.schemaCompliance(datafield)
                print(f"SUCCESS: Transformation of {datafield} Complete")
            else:
                print(f"WARNING: {datafield} Skipped - Empty DataFrame")

        print("SUCCESS: yFinance Transform Completed")
        print("ALERT: Errors saved to yfin_tranform_errors.csv")
        pd.DataFrame(self.errors_all).to_csv(
            "yfin_transform_errors.csv", index=False)
        return self.yfinance_data
