from abc import ABC, abstractmethod
from typing import List, Set


class InvertedIndexInterface(ABC):
    @abstractmethod
    def initialize(self) -> bool:
        pass

    @abstractmethod
    def add_document_to_term(self, term: str, book_id: int) -> bool:
        pass

    @abstractmethod
    def get_documents_for_term(self, term: str) -> Set[int]:
        pass

    @abstractmethod
    def get_all_terms(self) -> List[str]:
        pass

    @abstractmethod
    def get_index_size(self) -> int:
        pass

    @abstractmethod
    def close(self) -> None:
        pass