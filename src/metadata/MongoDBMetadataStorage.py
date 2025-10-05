import logging
from typing import List, Optional

try:
    from pymongo import MongoClient, ASCENDING
    from pymongo.errors import PyMongoError

    PYMONGO_AVAILABLE = True
except ImportError:
    PYMONGO_AVAILABLE = False
    logging.warning("pymongo not installed. MongoDB storage will not work.")

from src.metadata.MetadataStorageInterface import MetadataStorageInterface
from src.metadata.BookMetadata import BookMetadata


class MongoDBMetadataStorage(MetadataStorageInterface):
    def __init__(
            self,
            connection_string: str = "mongodb://localhost:27017/",
            database_name: str = "gutenberg_search",
            collection_name: str = "books"
    ):
        if not PYMONGO_AVAILABLE:
            raise ImportError(
                "pymongo is required for MongoDB storage. "
                "Install it with: pip install pymongo"
            )

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
                [("book_id", ASCENDING)],
                unique=True
            )

            self.collection.create_index([("author", ASCENDING)])

            self.collection.create_index([("language", ASCENDING)])

            self.collection.create_index([("title", "text")])

            logging.info(
                f"Initialized MongoDB metadata storage: "
                f"{self.database_name}.{self.collection_name}"
            )
            return True

        except PyMongoError as e:
            logging.error(f"Failed to initialize MongoDB metadata storage: {e}")
            return False

    def insert_book_metadata(self, metadata: BookMetadata) -> bool:
        if not self.collection:
            logging.error("Storage not initialized. Call initialize() first.")
            return False

        try:
            document = {
                "book_id": metadata.book_id,
                "title": metadata.title,
                "author": metadata.author,
                "language": metadata.language,
                "release_date": metadata.release_date
            }

            result = self.collection.update_one(
                {"book_id": metadata.book_id},
                {"$set": document},
                upsert=True
            )

            logging.debug(
                f"{'Updated' if result.matched_count else 'Inserted'} "
                f"metadata for book {metadata.book_id}"
            )
            return True

        except PyMongoError as e:
            logging.error(f"Failed to insert metadata for book {metadata.book_id}: {e}")
            return False

    def get_book_by_id(self, book_id: int) -> Optional[BookMetadata]:
        if not self.collection:
            logging.error("Storage not initialized. Call initialize() first.")
            return None

        try:
            document = self.collection.find_one({"book_id": book_id})

            if document:
                return self._document_to_metadata(document)
            return None

        except PyMongoError as e:
            logging.error(f"Failed to get book {book_id}: {e}")
            return None

    def get_books_by_author(self, author: str) -> List[BookMetadata]:
        if not self.collection:
            logging.error("Storage not initialized. Call initialize() first.")
            return []

        try:
            documents = self.collection.find(
                {"author": {"$regex": author, "$options": "i"}}
            ).sort("title", ASCENDING)

            return [self._document_to_metadata(doc) for doc in documents]

        except PyMongoError as e:
            logging.error(f"Failed to get books by author '{author}': {e}")
            return []

    def get_books_by_language(self, language: str) -> List[BookMetadata]:
        if not self.collection:
            logging.error("Storage not initialized. Call initialize() first.")
            return []

        try:
            documents = self.collection.find(
                {"language": language}
            ).sort("title", ASCENDING)

            return [self._document_to_metadata(doc) for doc in documents]

        except PyMongoError as e:
            logging.error(f"Failed to get books by language '{language}': {e}")
            return []

    def get_all_books(self, limit: Optional[int] = None) -> List[BookMetadata]:
        if not self.collection:
            logging.error("Storage not initialized. Call initialize() first.")
            return []

        try:
            cursor = self.collection.find().sort("book_id", ASCENDING)

            if limit:
                cursor = cursor.limit(limit)

            return [self._document_to_metadata(doc) for doc in cursor]

        except PyMongoError as e:
            logging.error(f"Failed to get all books: {e}")
            return []

    def get_total_books(self) -> int:
        if not self.collection:
            logging.error("Storage not initialized. Call initialize() first.")
            return 0

        try:
            return self.collection.count_documents({})

        except PyMongoError as e:
            logging.error(f"Failed to get total books count: {e}")
            return 0

    def book_exists(self, book_id: int) -> bool:
        if not self.collection:
            logging.error("Storage not initialized. Call initialize() first.")
            return False

        try:
            return self.collection.count_documents(
                {"book_id": book_id},
                limit=1
            ) > 0

        except PyMongoError as e:
            logging.error(f"Failed to check if book {book_id} exists: {e}")
            return False

    def close(self) -> None:
        if self.client:
            self.client.close()
            logging.info("Closed MongoDB metadata storage connection")

    @staticmethod
    def _document_to_metadata(document: dict) -> BookMetadata:
        return BookMetadata(
            book_id=document.get("book_id"),
            title=document.get("title"),
            author=document.get("author"),
            language=document.get("language"),
            release_date=document.get("release_date")
        )