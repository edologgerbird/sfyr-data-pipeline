'''
API for Google Cloud Firestore
'''

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
        print("INFO: Firestore initialised")

    # Adds a document to a specified collection.
    def fsAddDocument(self, collection, data):
        """Adds a document to a specified collection

        Args:
            collection (string): Name of collection
            data (dictionary): Data to be stored

        Returns:
            boolean: Success/Failure of Document Creation
        """
        self.db.collection(collection).add(data)
        print(f"SUCCESS: Document added to {collection}")
        return True

    # Adds multiple documents to a specified collection.
    def fsAddListofDocuments(self, collection, data_list):
        """Adds multiple documents to a specified collection

        Args:
            collection (string): Name of collection
            data (list): Data to be stored

        Returns:
            boolean: Success/Failure of Document Creation
        """
        for data in data_list:
            self.fsAddDocument(collection, data)
            print(f"SUCCESS: Document added to {collection}")
        return True

    # Sets a document in a specified collection, merge=False for overwriting.
    def fsSetDocument(self, collection, document, data, merge=True):
        """Sets a document in a specified collection

        Args:
            collection (string): Name of collection
            document (string): Name of document
            data (list): Data to be stored
            merge (bool, optional): merge=False for overwriting. Defaults to True.

        Returns:
            boolean: Success/Failure of Document Creation
        """
        self.db.collection(collection).document(document).set(data, merge)
        print(f"SUCCESS: Document set in {collection}")
        return True

    # Updates a single field within a specified doument
    def fsUpdateSingleField(self, collection, document, field, update):
        """Updates a single field within a specified doument

        Args:
            collection (string): Name of collection
            document (string): Name of document
            field (string): Field to Update
            update (any): New data to be updated

        Returns:
            boolean: Success/Failure of Update
        """
        self.db.collection(collection).document(document).update(
            {field: update}
        )
        print(f"SUCCESS: {field} updated to {update}")
        return True

    # Updates multiple fields within a specified doument
    def fsUpdateMultiFields(self, collection, document, update):
        """Updates multiple fields within a specified doument

        Args:
            collection (string): Name of collection
            document (string): Name of document
            update (dictionary): New data to be updated

        Raises:
            Exception: Update not in dictionary format

        Returns:
            boolean: Success/Failure of Update
        """
        if not isinstance(update, dict):
            raise Exception("Update not in dictionary format")
        self.db.collection(collection).document(document).update(update)
        print(f"SUCCESS: Multiple fields in {document} updated")
        return True

    # Adds elements into an array in a specified field
    def fsAddArrayElement(self, collection, document, field, add):
        """Adds elements into an array in a specified field

        Args:
            collection (string): Name of collection
            document (string): Name of document
            field (string): Field to Update
            add (any): New data to be added

        Returns:
            boolean: Success/Failure of Update
        """
        self.db.collection(collection).document(document).update(
            {field: firestore.ArrayUnion([add])}
        )
        print(f"SUCCESS: Array elements in {document} updated")
        return True

    # Removes elements in an array in a specified field
    def fsRemoveArrayElement(self, collection, document, field, remove):
        """Removes elements in an array in a specified field

        Args:
            collection (string): Name of collection
            document (string): Name of document
            field (string): Field to Update
            remove (any): Data to be removed

        Returns:
            boolean: Success/Failure of Update
        """
        self.db.collection(collection).document(document).update(
            {field: firestore.ArrayRemove([remove])}
        )
        print(f"SUCCESS: Array elements in {document} removed")
        return True

    # Increases a numeric field in a specified field
    def fsIncreaseNumeric(self, collection, document, field, increment):
        """ Increases a numeric field in a specified field

        Args:
            collection (string): Name of collection
            document (string): Name of document
            field (string): Field to Update
            increment (int/float): Amount to increment by

        Returns:
            boolean: Success/Failure of Update
        """
        self.db.collection(collection).document(document).update(
            {field: firestore.Increment(increment)}
        )
        print(f"SUCCESS: {field} in {document} increamented by {increment}")
        return True

    # Deletes specified documents
    def fsDeleteDocument(self, collection, documents):
        """Deletes specified documents

        Args:
            collection (string): Name of collection
            document (string): Name of document

        Returns:
            boolean: Success/Failure of Delete
        """
        for document in documents:
            self.db.collection(collection).document(document).delete()
        print(f"SUCCESS: {document} deleted from {collection}")
        return True

    # Deletes a specified field
    def fsDeleteField(self, collection, document, field):
        """Deletes a specified field

        Args:
            collection (string): Name of collection
            document (string): Name of document
            field (string): Field to Delete

        Returns:
            boolean: Success/Failure of Delete
        """
        self.db.collection(collection).document(document).update(
            {field: firestore.DELETE_FIELD}
        )
        print(f"SUCCESS: {field} deleted from {document} in {collection}")
        return True

    # Gets a specified document
    def fsGetDocument(self, collection, document):
        """Gets a specified document

        Args:
            collection (string): Name of collection
            document (string): Name of document

        Raises:
            Exception: Document does not exist!

        Returns:
            dictionary: Data in the document
        """
        doc = self.db.collection(collection).document(document).get()
        if doc.exists:
            return doc.to_dict()
        else:
            raise Exception("ERROR: Document does not exist!")

    # Gets a specified collection
    def fsGetCollection(self, collection):
        """ Gets a specified collection

        Args:
            collection (string): Name of collection

        Returns:
            list: list of dictionary of data in the collection
        """
        return [doc.to_dict() for doc in self.db.collection(collection).stream()]

    # Queries a specified document. Query format ('field', 'operator', 'criteria')
    def fsQueryDocuments(self, collection, *queries):
        """Queries a specified document. 

        Args:
            collection (string): Name of collection
            queries: Query format ('field', 'operator', 'criteria')

        Returns:
            List: List of data queried 
        """
        query_result = list()
        collection_ref = self.db.collection(collection)
        for query in queries:
            query_result = collection_ref.where(query[0], query[1], query[2])
        if query_result:
            print("SUCCESS: Data queried from {collection}")
            return [x.to_dict() for x in query_result.stream()]
        else:
            print("SUCCESS: Data queried from {collection}")
            return query_result
