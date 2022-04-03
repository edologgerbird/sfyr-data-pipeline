'''
Loading SBR Data to Firestor
'''

class FirestoreDataLoader:
    def __init__(self, firestoreDB):
        self.firestoreDB_layer = firestoreDB

    def upload_to_firestore(self, collection, data_to_upload):
        self.firestoreDB_layer.fsAddListofDocuments(
            collection, data_to_upload)
        print(f"{collection} successfully uploaded.")


