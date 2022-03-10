import pandas
import pandas_gbq
import json

class gbqInjest:
  def __init__(self):
    with open('serviceAccount.json', 'r') as jsonFile:
      self.cred = json.load(jsonFile)
    self.dataset_id = self.cred["DATASET_ID"]
    self.datasetTable = self.cred["DATASET_TABLE"]
    self.project_id = self.cred["PROJECT_ID"]

  def gbqCreateNewTable(self, data, datasetName, tableName):
    datasetTable = datasetName+ "." + tableName
    try:
      pandas_gbq.to_gbq(data, datasetTable, project_id=self.project_id)
      self.cred["TABLE_ID"].append(datasetTable)
      if (datasetName not in self):
        self.cred["DATASET_ID"].append(datasetName)
      with open('serviceAccount.json', 'r+') as jsonFile:
        json.dump(self.cred, jsonFile)
      return True
    except Exception as err:
      raise err    

  #datasetTableName is to be in the form of datasetName.TableName
  def gbqInjestAppend(self, data, datasetTable):
    if (datasetTable in self.datasetTable):
      try:
          pandas_gbq.to_gbq(data, datasetTable, project_id=self.project_id, if_exists="append")
          return True
      except Exception as err:
          raise err
    else: 
      raise Exception("Table Does not Exist")

  #datasetTable is to be in the form of datasetName.TableName
  def gbqInjestReplace(self, data, datasetTable):
    if (datasetTable in self.datasetTable):
      try:
          pandas_gbq.to_gbq(data, datasetTable, project_id=self.project_id, if_exists="replace")
          return True
      except Exception as err:
          raise err
    else: 
      raise Exception("Table Does not Exist")
  
  # Returns all datasetName as a list
  def getDataset(self):
    return self.dataset_id

  # Returns all datasetName.tableName as a list
  def getTables(self):
    return self.datasetTable

  

class gbqQuery:
  def __init__(self):
    with open('serviceAccount.json', 'r') as jsonFile:
      self.cred = json.load(jsonFile)
    self.datasetTable = self.cred["DATASET_TABLE"]
    self.project_id = self.cred["PROJECT_ID"]

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

# df = pandas.DataFrame(
#     {
#         'my_string': ['a', 'b', 'c'],
#         'my_int64': [1, 2, 3],
#         'my_float64': [4.0, 5.0, 6.0],
#         'my_timestamp': [
#             pandas.Timestamp("1998-09-04T16:03:14"),
#             pandas.Timestamp("2010-09-13T12:03:45"),
#             pandas.Timestamp("2015-10-02T16:00:00")
#         ],
#     }
# )

# print(gbqInjest().gbqInjestReplace(df,"test.test02"))

# Test Query Using SQL
# print(gbqQuery().getDataQuery("SELECT my_string FROM test.test02"))
# Test Query Using FieldName
# print(gbqQuery().getDataFields("test.test02","my_string","my_float64"))