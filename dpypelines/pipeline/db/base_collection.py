from abc import ABC
from dpytools.db.db_collection import DBCollection
from dpytools.db.documentdb_client import DocumentDBClient


class BaseCollection(ABC):
    """
    Base class to ensure that all child collection classes implement a method to initialise a DBCollection.
    """

    def __init__(self, client: DocumentDBClient, collection_name: str):
        self.client = client
        self.collection_name = collection_name

    def initialise_collection(self) -> DBCollection:
        """
        Initialise a DBCollection for the given collection name.

        :param client: The DocumentDBClient that manages the connection to the database.

        :return: dpytools.db.DBCollection
        """
        return self.client.get_collection(self.collection_name)
