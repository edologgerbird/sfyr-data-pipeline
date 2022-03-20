'''
API for Google Cloud Firestore
Functionalities:
-- Write
1. Create or overwrite document
2. Create without overwriting document
3. Update a field
4. Update nested field
5. Add elements in array
6. Remove elements in array
7. Increase a numeric value
8. Delete document
9. Delete field

-- Read
1. Get a document
2. Get multiple documents from a collection
3. Get all documents from a collection
4. List subcollections of a document

'''

from numpy import NaN
import pandas as pd
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore


class firestoreDB:
    def __init__(self):
        if not firebase_admin._apps:
            cred = credentials.Certificate(
                "utils/is3107-group-7-firebase-adminsdk-x2qta-d9cfd2898b.json"
            )
            firebase_admin.initialize_app(cred)
            self.db = firestore.client()

    # Adds a document to a specified collection.
    def fsAddDocument(self, collection, data):
        self.db.collection(collection).add(data)

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
        self.db.collection(collection).document(document).update(update)
        return True

    # Adds elements into an array in a specified field
    def fsAddArrayElement(self, collection, document, field, add):
        self.db.collection(collection).document(document).update(
            {field : firestore.ArrayUnion([add])}
        )
        return True

    # Returns elements into an array in a specified field
    def fsRemoveArrayElement(self, collection, document, field, remove):
        self.db.collection(collection).document(document).update(
            {field : firestore.ArrayRemove([remove])}
        )
        return True

    # Increases a numeric field in a specified field
    def fsIncreaseNumeric(self, collection, document, field, increment):
        self.db.collection(collection).document(document).update(
            {field : firestore.Increment(50)}
        )
        return True

    # Deletes a specified document
    def fsDeleteDocument(self, collection, document):
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
            return None

    # Gets a specified collection
    def fsGetCollection(self, collection):
        return list(doc.to_dict() for doc in self.db.collection(collection).stream())

    # Queries a specified document. Query format 
    def fsQueryDocuments(self, collection, *queries):
        collection_ref = self.db.collection(collection) 
        for query in queries:
            query_result = collection_ref.where(query[0], query[1], query[2])
        return [x.to_dict() for x in query_result.stream()]
        


# Testing 

test_data = {
    "text_headline" : "HEADLINES HERE 2134",
    "text_body": "lorem ipsum si dollar amet 1234",
    "date": "24/12/21",
    "companies": {
        "S69": "Wilmar",
        "DF3": "ComfortDelgro",
        "GDF": "GoodDay Friday"
    },
    "STI_movement" : {
        "direction" : "positive",
        "percentage" : "15%"
    }

}

db = firestoreDB()
db.fsAddDocument("test_collection", test_data)
#print(db.fsGetCollection("test_collection"))
print(db.fsQueryDocuments("test_collection", ("companies.S69", "!=", 0)))


    
        
    








# doc_ref = self.db.collection(u'users').document(u'alovelace')
# doc_ref.set({
#     u'first': u'Ada',
#     u'last': u'Lovelace',
#     u'born': 1815
# })


