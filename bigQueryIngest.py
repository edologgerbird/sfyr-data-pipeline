import pandas
import pandas_gbq
import json

with open('serviceAccount.json', 'r') as json_file:
  cred = json.load(json_file)

table_id = cred["TABLE_ID"]
project_id = cred["PROJECT_ID"]

def gbqInjest(data):
    # Assumption: data taken in as pandas dataframe
    try:
        pandas_gbq.to_gbq(data, table_id, project_id=project_id, if_exist = "append")
        print("Success")
    except:
        print("Failed to Send Data")