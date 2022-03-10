from google.cloud import bigquery
import pandas
import pandas_gbq
import json
class gbdAPI:
  def __init__(self):
    with open('serviceAccount.json', 'r') as jsonFile:
      self.cred = json.load(jsonFile)
    self.table_id = self.cred["TABLE_ID"]
    self.project_id = self.cred["PROJECT_ID"]

  def gbqCreateNewTable(self, data, datasetName, tableName):
    datasetTable = datasetName+"." + tableName
    try:
      pandas_gbq.to_gbq(data, datasetTable, project_id=self.project_id)
      self.cred["TABLE_ID"].append(datasetTable)
      with open('serviceAccount.json', 'r+') as jsonFile:
        json.dump(self.cred, jsonFile)
      return True
    except Exception as err:
      return err    

  #datasetTable is to be in the form of datasetName.TableName
  def gbqInjestAppend(self, data, datasetTable):
    if (datasetTable in self.table_id):
      try:
          pandas_gbq.to_gbq(data, datasetTable, project_id=self.project_id, if_exists="append")
          return True
      except Exception as err:
          return err
    else: 
      return Exception("Table Does not Exist")

  #datasetTable is to be in the form of datasetName.TableName
  def gbqInjestReplace(self, data, datasetTable):
    if (datasetTable in self.table_id):
      try:
          pandas_gbq.to_gbq(data, datasetTable, project_id=self.project_id, if_exists="replace")
          return True
      except Exception as err:
          return err
    else: 
      return Exception("Table Does not Exist")

  
  # Returns all datasetName.tableName as a list
  def getTables(self):
    return self.table_id


df = pandas.DataFrame(
    {
        'my_string': ['a', 'b', 'c'],
        'my_int64': [1, 2, 3],
        'my_float64': [4.0, 5.0, 6.0],
        'my_timestamp': [
            pandas.Timestamp("1998-09-04T16:03:14"),
            pandas.Timestamp("2010-09-13T12:03:45"),
            pandas.Timestamp("2015-10-02T16:00:00")
        ],
    }
)

print(gbdAPI().gbqInjestReplace(df,"test.test02"))