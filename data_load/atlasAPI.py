import pymongo
from pymongo import MongoClient

user = "is3107_g7"
password = "lBNGeYxMOsgvjKPH"
cluster = "is3107-g7-c1"

client = pymongo.MongoClient("mongodb+srv://is3107_g7:hellothere@is3107-g7-c1.jfhak.mongodb.net/")
#db = client.test

# cluster = MongoClient(f"mongodb+srv://{user}:{password}@{cluster}.jfhak.mongodb.net/is3107-g7?retryWrites=true&w=majority")

# client = pymongo.MongoClient("mongodb+srv://is3107_g7:lBNGeYxMOsgvjKPH@is3107-g7-c1.jfhak.mongodb.net/is3107-g7?ssl_cert_reqs=CERT_NONE", connect=False)
# # db = client.test

# db = cluster["is3107-g7"]
# collection = db["test"]

# print(collection)
# collection.insert_one({"_id":0, "user_name":"zhouwai"})
# collection.insert_one({"_id":100, "user_name":"bong"})
for name in client.list_database_names():  
    print(name) 

