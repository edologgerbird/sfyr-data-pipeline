import pandas as pd
import pandas_gbq
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import time


class bigQueryDB:
    def __init__(self):
        print("INFO: Initialising GBQ Pipeline...")
        # Set-up Credentials and Project
        self.credentials = service_account.Credentials.from_service_account_file(
            'utils/is3107-group-7-008534a376ad.json',)
        self.client = bigquery.Client(credentials=self.credentials)
        self.project = self.client.project

        # Set-up Local Config
        self.credUrl = "utils/serviceAccount.json"
        with open(self.credUrl, 'r') as jsonFile:
            self.cred = json.load(jsonFile)
        self.project_id = self.cred["bigQueryConfig"]["PROJECT_ID"]
        self.dataset_id = self.cred["bigQueryConfig"]["DATASET_ID"]
        self.datasetTable = self.cred["bigQueryConfig"]["DATASET_TABLE"]

        print("INFO: GBQ Pipeline Initialised")

    def gbqCreateNewTable(self, data, datasetName, tableName):
        datasetTable = datasetName + "." + tableName
        if (datasetTable not in self.datasetTable):
            if isinstance(data, pd.DataFrame):
                if not data.empty:
                    try:
                        pandas_gbq.to_gbq(
                            data, datasetTable, project_id=self.project_id, credentials=self.credentials)
                        print(f"SUCCESS: {datasetTable} Created")
                        print(f"SUCCESS: Data Added to {datasetTable}")
                        # Sync Local Dataset and Dataset_Table List - SyncTables Calls SyncDataSet
                        self.syncTables()
                        return True
                    except Exception as err:
                        self.syncTables()
                        print(
                            f"ERROR: Data Addition to {datasetTable} Aborted")
                        if datasetTable in self.datasetTable:
                            print(f"INFO: {datasetTable} has been created")
                        print(f"Error Log: {err}")
                        return False

                else:
                    print("ERROR: Empty DataFrame - Table Creation Aborted")
                    return False
            else:
                print("ERROR: Data File not Dataframe")
                return False
        else:
            print(
                f"INFO: {datasetTable} exist - Data Will Be Replaced unless Operation Cancelled")
            time.sleep(3)
            self.gbqReplace(data, datasetTable)

    # datasetTableName is to be in the form of datasetName.TableName
    def gbqAppend(self, data, datasetTable):
        # Sync Local Dataset and Dataset_Table List - SyncTables Calls SyncDataSet
        if (datasetTable in self.datasetTable):
            if isinstance(data, pd.DataFrame):
                if not data.empty:
                    try:
                        pandas_gbq.to_gbq(data, datasetTable, project_id=self.project_id,
                                          if_exists="append", credentials=self.credentials)
                        print(
                            f"SUCCESS: Data Appended to {datasetTable}")
                        return True
                    except Exception as err:
                        print(f"ERROR: Data Append to {datasetTable} Aborted")
                        print(f"Error Log: {err}")
                        return False
                else:
                    print("ERROR: Empty DataFrame - Table Creation Aborted")
            else:
                print("ERROR: Data Object not Dataframe")
                return False
        else:
            print(
                f"INFO: {datasetTable} does not exist - Table will be created unless Operation Cancelled")
            time.sleep(3)
            datasetTableSplit = datasetTable.split(".")
            self.gbqCreateNewTable(
                data, datasetTableSplit[0], datasetTableSplit[1])

    # datasetTable is to be in the form of datasetName.TableName
    def gbqReplace(self, data, datasetTable):
        if (datasetTable in self.datasetTable):
            if isinstance(data, pd.DataFrame):
                if not data.empty:
                    try:
                        pandas_gbq.to_gbq(data, datasetTable, project_id=self.project_id,
                                          if_exists="replace", credentials=self.credentials)
                        print(f"SUCCESS: {datasetTable} Data Replaced")
                        return True
                    except Exception as err:
                        print(f"ERROR: Data Append to {datasetTable} Aborted")
                        print(f"Error Log: {err}")
                        return False
                else:
                    print("Empty Dataset - Replace Aborted")
            else:
                print("ERROR: Data Object not Dataframe")
                return False
        else:
            print(
                f"INFO: {datasetTable} does not exist - Table will be created unless Operation Cancelled")
            time.sleep(3)
            datasetTableSplit = datasetTable.split(".")
            self.gbqCreateNewTable(
                data, datasetTableSplit[0], datasetTableSplit[1])

    def gbqDeleteDataset(self, dataset):
        try:
            self.client.delete_table(dataset, delete_contents=True)
            self.syncDataset()
            return True
        except Exception as err:
            self.syncDataset()
            raise err

    def gbqDeleteTable(self, datasetTable):
        try:
            self.client.delete_table(datasetTable)
            self.syncTables()
            return True
        except Exception as err:
            self.syncTables()
            raise err

    def gbqCheckDatasetExist(self, datasetName):
        gbqDatasets = self.getDataset()
        return datasetName in gbqDatasets

    def gbqCheckTableExist(self, datasetTable):
        gbqTables = self.getTables()
        return datasetTable in gbqTables

    def gbqQueryAPI(self, query):
        try:
            df = pandas_gbq.read_gbq(
                query, project_id=self.project_id, credentials=self.credentials)
            return df

        except Exception as err:
            raise err

    # queryString takes in SQL Queries
    def getDataQuery(self, queryString):
        sql = ""+queryString+""
        return self.gbqQueryAPI(sql)

    def getDataFields(self, datasetTable, *fields):
        fieldString = ""
        if fields:
            for field in fields:
                fieldString = fieldString + field + ", "
            fieldString = fieldString[:len(fieldString)-2]
        else:
            fieldString = "*"
        queryString = "SELECT " + fieldString + " FROM " + datasetTable
        sql = "" + queryString + ""
        return self.gbqQueryAPI(sql)

    # Returns all datasetName as a list
    def getDataset(self):
        return self.syncDataset()

    # Helper Function to Sync Local Dataset with Cloud
    def syncDataset(self):
        datasets = list(self.client.list_datasets())  # Make an API request.
        print(f"INFO: Updating Datasets for {datasets}")
        updatedDatasetList = []

        if datasets:
            for dataset in datasets:
                updatedDatasetList.append(dataset.dataset_id)
            # Updating serviceAccount.json
            self.cred["bigQueryConfig"]["DATASET_ID"] = updatedDatasetList
            self.dataset_id = updatedDatasetList

            with open(self.credUrl, 'w') as jsonFile:
                json.dump(self.cred, jsonFile)
            print("SUCCESS: Dataset Succesfully Updated")
            return updatedDatasetList

        else:
            print("WARNING: {} does not contain any datasets.".format(self.project))
            return None

    # Returns all datasetName.tableName as a list
    def getTables(self):
        return self.syncTables()

    # Helper Function to Sync Local Tables with Cloud
    def syncTables(self):
        updatedTableList = []
        self.syncDataset()
        print(f"INFO: Updating Tables for {self.dataset_id}")
        for dataset in self.dataset_id:
            try:
                tables = self.client.list_tables(
                    dataset)  # Make an API request
                for table in tables:
                    updatedTableList.append(
                        table.dataset_id + "." + table.table_id)

            except Exception as err:
                print(f"ERROR: {dataset} Update Failure")
                print(f"Error Log: {err}")

        # Updating serviceAccount.json
        self.cred["bigQueryConfig"]["DATASET_TABLE"] = updatedTableList

        with open(self.credUrl, 'w') as jsonFile:
            json.dump(self.cred, jsonFile)

        print("INFO: Tables Succesfully Updated")
        return updatedTableList

    # Returns a list of schemaField
    def getTableSchema(self, datasetTable):
        queryString = "SELECT * FROM " + datasetTable
        query = "" + queryString + ""
        query_job = self.client.query(query)
        result = query_job.result()
        schema = result.schema
        return schema

    def getTableColumns(self, datasetTable):
        # --- Schema Field Attributes
        # field_type: data type of column
        # name: name of column
        schema = self.getTableSchema(datasetTable)
        column_names = [schemafield.name for schemafield in schema]
        data_type = [schemafield.field_type for schemafield in schema]
        return [column_names, data_type]


#------- Unit Test Codes -----------#

# df = pd.DataFrame(
#     {
#         'my_string': ['a', 'b', 'c'],
#         'my_int64': [1, 2, 3],
#         'my_float64': [4.0, 5.0, 6.0],
#         'my_timestamp': [
#             pd.Timestamp("1998-09-04T16:03:14"),
#             pd.Timestamp("2010-09-13T12:03:45"),
#             pd.Timestamp("2015-10-02T16:00:00")
#         ],
#     }
# )


# Test Query Using SQL
# print(self.getDataQuery("SELECT my_string FROM test.test02"))
# Test Query Using FieldName
# print(self.getDataFields("test.test02","my_string","my_float64"))
