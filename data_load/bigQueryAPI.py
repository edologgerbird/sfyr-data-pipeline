import pandas as pd
import pandas_gbq
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import os

class gbqInjest:
  def __init__(self):
    self.credUrl = "../utils/serviceAccount.json"
    with open(self.credUrl, 'r') as jsonFile:
      self.cred = json.load(jsonFile)
    self.dataset_id = self.cred["bigQueryConfig"]["DATASET_ID"]
    self.datasetTable = self.cred["bigQueryConfig"]["DATASET_TABLE"]
    self.project_id = self.cred["bigQueryConfig"]["PROJECT_ID"]
    self.credentials = service_account.Credentials.from_service_account_file('../utils/is3107-group-7-008534a376ad.json',)
  
  def gbqCreateNewTable(self, data, datasetName, tableName):
    datasetTable = datasetName+ "." + tableName
    if isinstance(data, pd.DataFrame):
      try:
        pandas_gbq.to_gbq(data, datasetTable, project_id=self.project_id)
        self.cred["bigQueryConfig"]["TABLE_ID"].append(datasetTable)
        if (datasetName not in self):
          self.cred["bigQueryConfig"]["DATASET_ID"].append(datasetName)
        with open(self.credUrl, 'w') as jsonFile:
          json.dump(self.cred, jsonFile)
        return True
      except Exception as err:
        raise err    
    else: 
      raise Exception("Data File not Dataframe")

  #datasetTableName is to be in the form of datasetName.TableName
  def gbqInjestAppend(self, data, datasetTable):
    if (datasetTable in self.datasetTable):
      if isinstance(data, pd.DataFrame):
        try:
            pandas_gbq.to_gbq(data, datasetTable, project_id=self.project_id, if_exists="append")
            return True
        except Exception as err:
            raise err
      else: 
        raise Exception("Data File not Dataframe")
    else: 
      raise Exception("Table Does not Exist")

  #datasetTable is to be in the form of datasetName.TableName
  def gbqInjestReplace(self, data, datasetTable):
    if (datasetTable in self.datasetTable):
      if isinstance(data, pd.DataFrame):
        try:
            pandas_gbq.to_gbq(data, datasetTable, project_id=self.project_id, if_exists="replace")
            return True
        except Exception as err:
            raise err
      else: 
        raise Exception("Data File not Dataframe")
    else: 
      raise Exception("Table Does not Exist")
  
  # Returns all datasetName as a list
  def getDataset(self):
    datasets = self.syncDataset()
    if datasets == None:
      print("Project dues to contain any datasets")
    else:
      print(datasets)
      self.cred["bigQueryConfig"]["DATASET_ID"] = datasets
      print(self.cred)
      with open(self.credUrl, 'w') as jsonFile:
        json.dump(self.cred, jsonFile)
      return datasets

  # Returns all datasetName.tableName as a list
  def getTables(self):
    return self.datasetTable

  def syncDataset(self):
    client = bigquery.Client(credentials=self.credentials)
    datasets = list(client.list_datasets())  # Make an API request.
    project = client.project
    datasetList = []
    if datasets:
      print("Datasets in project {}:".format(project))
      for dataset in datasets:
        print("\t{}".format(dataset.dataset_id))
        datasetList.append(dataset.dataset_id)
      return datasetList

    else:
      print("{} project does not contain any datasets.".format(project))
      return None

class gbqQuery:
  def __init__(self):
    with open(self.credUrl, 'r') as jsonFile:
      self.cred = json.load(jsonFile)
    self.dataset_id = self.cred["bigQueryConfig"]["DATASET_ID"]
    self.datasetTable = self.cred["bigQueryConfig"]["DATASET_TABLE"]
    self.project_id = self.cred["bigQueryConfig"]["PROJECT_ID"]

  def gbdQueryAPI(self, query):
    try: 
      df = pandas_gbq.read_gbq(query, project_id=self.project_id)
      return df
    except Exception as err: 
      raise err

  # queryString takes in SQL Queries
  def getDataQuery(self, queryString):
    sql = ""+queryString+""
    return self.gbdQueryAPI(sql)


  def getDataFields(self,datasetTable, *fields):
    fieldString = ""
    
    for field in fields:
      fieldString = fieldString + field + ", "
    fieldString = fieldString[:len(fieldString)-2]

    queryString = "SELECT " + fieldString + " FROM " + datasetTable
    print(queryString)

    sql = "" + queryString + ""

    return self.gbdQueryAPI(sql)


#------- Test Codes -----------#

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

# print(gbqInjest().gbqInjestReplace(df,"test.test02"))
print(gbqInjest().getDataset())

# Test Query Using SQL
# print(gbqQuery().getDataQuery("SELECT my_string FROM test.test02"))
# Test Query Using FieldName
# print(gbqQuery().getDataFields("test.test02","my_string","my_float64"))