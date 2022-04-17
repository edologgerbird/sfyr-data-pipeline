import pandas as pd
import pandas_gbq
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import time
import traceback


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

        # Set-up Table Schema
        self.tableSchemaUrl = "utils/bigQuerySchema.json"
        with open(self.tableSchemaUrl, 'r') as schemaFile:
            self.tableSchema = json.load(schemaFile)

        print("INFO: GBQ Pipeline Initialised")

    def gbqCreateNewTable(self, data, datasetName, tableName):
        """This function creates a new table in BigQuery

        Args:
            data (dataframe): Data to be stored in the table
            datasetName (string): Name of dataset. Can be new or existing
            tableName (string): Name of dataset. Must be new

        Returns:
            boolean: Success/Failure of table creation
        """
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
                        print(
                            f"ERROR: Data Addition to {datasetTable} Aborted")
                        print(f"Error Log: {err}")
                        print(f"Traceback: {traceback.format_exc()}")
                        self.syncTables()
                        if datasetTable in self.datasetTable:
                            print(f"ALERT: {datasetTable} has been created")
                        return False

                else:
                    print("ERROR: Empty DataFrame - Table Creation Aborted")
                    return False
            else:
                print("ERROR: Data Object Not Dataframe")
                return False
        else:
            print(
                f"INFO: {datasetTable} exist - Data Will Be Appended unless Operation Cancelled")
            time.sleep(7)
            self.updateTableSchema([datasetTable])
            self.gbqAppend(data, datasetTable, self.tableSchema[datasetTable])

    # datasetTableName is to be in the form of datasetName.TableName
    def gbqAppend(self, data, datasetTable, schema=None):
        """This function appends data to an existing table in BigQuery

        Args:
            data (dataframe): Data to be stored in the table
            datasetTable (string): Name of datasetTable. Format dataset.Table
            schema (array, optional): _description_. Defaults to None.

        Returns:
            boolean: Success/Failure of data append
        """
        # Sync Local Dataset and Dataset_Table List - SyncTables Calls SyncDataSet
        if (datasetTable in self.datasetTable):
            if isinstance(data, pd.DataFrame):
                if not data.empty:
                    try:
                        if schema == None:
                            print(f"INFO: Schema Not Provided")
                            pandas_gbq.to_gbq(data, datasetTable, project_id=self.project_id,
                                              if_exists="append", credentials=self.credentials)
                        else:
                            print(f"INFO: Schema Provided {schema}")
                            pandas_gbq.to_gbq(data, datasetTable, project_id=self.project_id,
                                              if_exists="append", table_schema=schema, credentials=self.credentials)
                        print(
                            f"SUCCESS: Data Appended to {datasetTable}")
                        return True
                    except Exception as err:
                        print(f"ERROR: Data Append to {datasetTable} Aborted")
                        print(f"Error Log: {err}")
                        print(f"Traceback: {traceback.format_exc()}")
                else:
                    print("ERROR: Empty DataFrame - Table Creation Aborted")
            else:
                print("ERROR: Data Object Not Dataframe")
                return False
        else:
            print(
                f"INFO: {datasetTable} does not exist - Table will be created unless Operation Cancelled")
            time.sleep(7)
            datasetTableSplit = datasetTable.split(".")
            self.gbqCreateNewTable(
                data, datasetTableSplit[0], datasetTableSplit[1])

    # datasetTable is to be in the form of datasetName.TableName
    def gbqReplace(self, data, datasetTable, schema=None):
        """This function replaces data in an existing table in BigQuery

        Args:
            data (dataframe): Data to be stored in the table
            datasetTable (string): Name of datasetTable. Format dataset.Table
            schema (array, optional): _description_. Defaults to None.

        Returns:
            boolean: Success/Failure of data append
        """
        if (datasetTable in self.datasetTable):
            if isinstance(data, pd.DataFrame):
                if not data.empty:
                    try:
                        if schema == None:
                            print(f"INFO: Schema Not Provided")
                            pandas_gbq.to_gbq(data, datasetTable, project_id=self.project_id,
                                              if_exists="replace", credentials=self.credentials)
                        else:
                            print(f"INFO: Schema Provided {schema}")
                            pandas_gbq.to_gbq(data, datasetTable, project_id=self.project_id,
                                              if_exists="replace", table_schema=schema, credentials=self.credentials)
                        print(f"SUCCESS: {datasetTable} Data Replaced")
                        return True
                    except Exception as err:
                        print(f"ERROR: Data Append to {datasetTable} Aborted")
                        print(f"Error Log: {err}")
                        print(f"Traceback: {traceback.format_exc()}")
                        return False
                else:
                    print("Empty Dataset - Replace Aborted")
            else:
                print("ERROR: Data Object not Dataframe")
                return False
        else:
            print(
                f"INFO: {datasetTable} does not exist - Table will be created unless Operation Cancelled")
            time.sleep(7)
            datasetTableSplit = datasetTable.split(".")
            self.gbqCreateNewTable(
                data, datasetTableSplit[0], datasetTableSplit[1])

    def gbqDeleteDataset(self, dataset):
        """This function deletes dataset in BigQuery

        Args:
            dataset (string): Name of dataset.

        Returns:
            boolean: Success/Failure of action
        """
        try:
            self.client.delete_table(dataset, delete_contents=True)
            self.syncDataset()
            return True
        except Exception as err:
            self.syncDataset()
            print(f"ERROR: Delete {dataset} Aborted")
            print(f"Error Log: {err}")
            print(f"Traceback: {traceback.format_exc()}")
            return False

    def gbqDeleteTable(self, datasetTable):
        """This function deletes Table in BigQuery

        Args:
            datasetTable (string): Name of datasetTable. Format dataset.Table

        Returns:
            boolean: Success/Failure of action
        """
        try:
            self.client.delete_table(datasetTable)
            self.syncTables()
            return True
        except Exception as err:
            self.syncTables()
            print(f"ERROR: Delete {datasetTable} Aborted")
            print(f"Error Log: {err}")
            print(f"Traceback: {traceback.format_exc()}")
            return False

    def gbqCheckDatasetExist(self, datasetName):
        """This function checks if a dataset exists in BigQuery

        Args:
            datasetName (string): Name of dataset.

        Returns:
            boolean: Dataset exist or not exist
        """
        gbqDatasets = self.getDataset()
        return datasetName in gbqDatasets

    def gbqCheckTableExist(self, datasetTable):
        """This function checks if a table exists in BigQuery

        Args:
            datasetTable (_type_): Name of datasetTable. Format dataset.Table

        Returns:
            boolean: Table exist or not exist
        """
        gbqTables = self.getTables()
        return datasetTable in gbqTables

    def gbqQueryAPI(self, query):
        """This helper function makes a query to BigQuery to obtain data 

        Args:
            query (string): String of Query to be conducted in BigQuery

        Returns:
            dataframe: Query Response
        """
        print(f"INFO: Querying {query}")
        try:
            df = pandas_gbq.read_gbq(
                query, project_id=self.project_id, credentials=self.credentials)
            print("SUCCESS: Query Success")
            return df

        except Exception as err:
            print(f"ERROR: Query Aborted - Check Query Format {query} ")
            print(f"Error Log: {err}")
            print(f"Traceback: {traceback.format_exc()}")
            return False

    # queryString takes in SQL Queries
    def getDataQuery(self, queryString):
        """This helper function takes in a SQL

        Args:
            queryString (string): String of Query to be conducted in BigQuery

        Returns:
            dataframe: Query Response
        """
        sql = ""+queryString+""
        return self.gbqQueryAPI(sql)

    def getDataFields(self, datasetTable, *fields):
        """This function takes in a table and fields to be queried,  generates a SQL Query and queries BigQuery 

        Args:
            datasetTable (string): Name of datasetTable. Format dataset.Table
            fields (string): Name of fields to be extracted.

        Returns:
            dataframe: Query Response
        """
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
        """This function returns all dataset as a list

        Returns:
            list: List of all datasets in BigQuery
        """
        return self.syncDataset()

    # Helper Function to Sync Local Dataset with Cloud
    def syncDataset(self):
        """This helper funcction Sync Local Dataset with Cloud

        Returns:
            list: List of all datasets
        """
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
        """This function returns all tables as a list

        Returns:
            list: List of all Tables in BigQuery
        """
        return self.syncTables()

    # Helper Function to Sync Local Tables with Cloud
    def syncTables(self):
        """This helper funcction Sync Local Table with Cloud

        Returns:
            list: List of all Tables
        """
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

        print("SUCCESS: Tables Succesfully Updated")
        return updatedTableList

    # Returns a list of schemaField
    def getTableSchema(self, datasetTable):
        """This helper function queries BigQuery and returns Table Schema

        Args:
            datasetTable (string): Name of datasetTable. Format dataset.Table

        Returns:
            list: list of dictionary where each dictionary is schema for a field
        """
        print(f"INFO: Query Table Schema for {datasetTable}")
        queryString = "SELECT * FROM " + datasetTable
        query = "" + queryString + ""
        query_job = self.client.query(query)
        result = query_job.result()
        schemas = result.schema

        # --- Schema Field Attributes
        # field_type: data type of column
        # name: name of column

        formatted_schema = []
        for schema in schemas:
            schema_details = {
                'name': schema.name,
                'type': schema.field_type
            }
            formatted_schema.append(schema_details)

        print(f"SUCCESS: Retrived Table Schema for {datasetTable}")
        return formatted_schema

    def updateTableSchema(self, datasetTablelist):
        """This function queries BigQuery and updates local copy of Table Schema

        Args:
            datasetTablelist (list): List of datasetTable Names. Format dataset.Table

        Returns:
            boolean: Success/Failure of updating
        """
        print(f"INFO: Updating Table Schema for {datasetTablelist}")
        for datasetTable in datasetTablelist:
            updatedSchema = self.getTableSchema(datasetTable)
            self.tableSchema[datasetTable] = updatedSchema

        with open(self.tableSchemaUrl, 'w') as schemaFile:
            json.dump(self.tableSchema, schemaFile)

        print(f"SUCCESS: Updated Table Schema for {datasetTablelist} ")

        return True


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
