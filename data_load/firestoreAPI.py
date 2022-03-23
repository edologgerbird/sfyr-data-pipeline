'''
API for Google Cloud Firestore
'''

import pandas as pd
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore


class firestoreDB:
    def __init__(self):
        self.db = None
        if not firebase_admin._apps:
            cred = credentials.Certificate(
                "utils/is3107-group-7-firebase-adminsdk-x2qta-d9cfd2898b.json"
            )
            firebase_admin.initialize_app(cred)
            self.db = firestore.client()
        if self.db is None:
            raise Exception("firestore DB failed to initialise.")

    # Adds a document to a specified collection.
    def fsAddDocument(self, collection, data):
        self.db.collection(collection).add(data)
        return True

    # Sets a document in a specified collection, merge=False for overwriting.
    def fsSetDocument(self, collection, document, data, merge=True): 
        self.db.collection(collection).document(document).set(data, merge) 
        return True

    # Updates a single field within a specified doument
    def fsUpdateSingleField(self, collection, document, field, update):
        self.db.collection(collection).document(document).update(
            {field:update}
        )
        return True

    # Updates multiple fields within a specified doument
    def fsUpdateMultiFields(self, collection, document, update):
        if not isinstance(update, dict):
            raise Exception("Update not in dictionary format")
        self.db.collection(collection).document(document).update(update)
        return True

    # Adds elements into an array in a specified field
    def fsAddArrayElement(self, collection, document, field, add):
        self.db.collection(collection).document(document).update(
            {field : firestore.ArrayUnion([add])}
        )
        return True

    # Removes elements in an array in a specified field
    def fsRemoveArrayElement(self, collection, document, field, remove):
        self.db.collection(collection).document(document).update(
            {field : firestore.ArrayRemove([remove])}
        )
        return True

    # Increases a numeric field in a specified field
    def fsIncreaseNumeric(self, collection, document, field, increment):
        self.db.collection(collection).document(document).update(
            {field : firestore.Increment(increment)}
        )
        return True

    # Deletes specified documents
    def fsDeleteDocument(self, collection, documents):
        for document in documents:
            self.db.collection(collection).document(document).delete()
        return True

    # Deletes a specified field
    def fsDeleteField(self, collection, document, field):
        self.db.collection(collection).document(document).update(
            {field:firestore.DELETE_FIELD}
        )
        return True
    
    # Gets a specified document
    def fsGetDocument(self, collection, document):
        doc = self.db.collection(collection).document(document).get()
        if doc.exists:
            return doc.to_dict()
        else:
            raise Exception("Document does not exist!")

    # Gets a specified collection
    def fsGetCollection(self, collection):
        return [doc.to_dict() for doc in self.db.collection(collection).stream()]

    # Queries a specified document. Query format 
    def fsQueryDocuments(self, collection, *queries):
        collection_ref = self.db.collection(collection) 
        for query in queries:
            query_result = collection_ref.where(query[0], query[1], query[2])
        return [x.to_dict() for x in query_result.stream()]
        


# # Testing 

# test_data = {
#     "text_headline" : "HEADLINES HERE 2134",
#     "text_body": "lorem ipsum si dollar amet 1234",
#     "date": "24/12/21",
#     "company_codes" : ["S69", "DF3"],
#     "STI_movement" : {
#         "direction" : "positive",
#         "percentage_change" : 15
#     }

# }

# db = firestoreDB()
# db.fsAddDocument("test_collection", test_data)
# #print(db.fsGetCollection("test_collection"))
# print(db.fsQueryDocuments("test_collection", ("company_codes", "array_contains", "S69")))
