import logging
from typing import List, Set
from pymongo import MongoClient, ASCENDING
from pymongo.errors import PyMongoError
from src.indexer.InvertedIndexInterface import InvertedIndexInterface


class MongoDBInvertedIndex(InvertedIndexInterface):
    def __init__(
            self,
            connection_string: str = "mongodb://localhost:27017/",
            database_name: str = "inverted_index",
            collection_name: str = "words"
    ):
        self.connection_string = connection_string
        self.database_name = database_name
        self.collection_name = collection_name

        self.client = None
        self.db = None
        self.collection = None

    def initialize(self) -> bool:
        try:
            self.client = MongoClient(self.connection_string)
            self.client.server_info()

            self.db = self.client[self.database_name]
            self.collection = self.db[self.collection_name]

            self.collection.create_index(
                [("term", ASCENDING)],
                unique=True
            )

            logging.info(
                f"Initialized MongoDB inverted index: "
                f"{self.database_name}.{self.collection_name}"
            )
            return True

        except PyMongoError as e:
            logging.error(f"Failed to initialize MongoDB inverted index: {e}")
            return False

    def add_document_to_term(self, term: str, book_id: int) -> bool:
        if self.collection is None:
            logging.error("Index not initialized. Call initialize() first.")
            return False

        try:
            self.collection.update_one(
                {"term": term},
                {
                    "$addToSet": {"postings": book_id}
                },
                upsert=True
            )

            return True

        except PyMongoError as e:
            logging.error(f"Failed to add document {book_id} to term '{term}': {e}")
            return False

    def get_documents_for_term(self, term: str) -> Set[int]:
        if self.collection is None:
            logging.error("Index not initialized. Call initialize() first.")
            return set()

        try:
            document = self.collection.find_one({"term": term})

            if document and "postings" in document:
                return set(document["postings"])
            return set()

        except PyMongoError as e:
            logging.error(f"Failed to get documents for term '{term}': {e}")
            return set()

    def get_all_terms(self) -> List[str]:
        if self.collection is None:
            logging.error("Index not initialized. Call initialize() first.")
            return []

        try:
            cursor = self.collection.find(
                {},
                {"term": 1, "_id": 0}
            ).sort("term", ASCENDING)

            return [doc["term"] for doc in cursor]

        except PyMongoError as e:
            logging.error(f"Failed to get all terms: {e}")
            return []

    def get_index_size(self) -> int:
        if self.collection is None:
            logging.error("Index not initialized. Call initialize() first.")
            return 0

        try:
            return self.collection.count_documents({})

        except PyMongoError as e:
            logging.error(f"Failed to get index size: {e}")
            return 0

    def close(self) -> None:
        if self.client:
            self.client.close()
            logging.info("Closed MongoDB inverted index connection")