from abc import ABC, abstractmethod
from typing import List, Optional
from src.metadata.BookMetadata import BookMetadata


class MetadataStorageInterface(ABC):
    @abstractmethod
    def initialize(self) -> bool:
        pass

    @abstractmethod
    def insert_book_metadata(self, metadata: BookMetadata) -> bool:
        pass

    @abstractmethod
    def get_book_by_id(self, book_id: int) -> Optional[BookMetadata]:
        pass

    @abstractmethod
    def get_books_by_author(self, author: str) -> List[BookMetadata]:
        pass

    @abstractmethod
    def get_books_by_language(self, language: str) -> List[BookMetadata]:
        pass

    @abstractmethod
    def get_all_books(self, limit: Optional[int] = None) -> List[BookMetadata]:
        pass

    @abstractmethod
    def get_total_books(self) -> int:
        pass

    @abstractmethod
    def book_exists(self, book_id: int) -> bool:
        pass

    @abstractmethod
    def close(self) -> None:
        pass