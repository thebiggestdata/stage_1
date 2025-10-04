import logging
from pathlib import Path
from typing import List, Set

from src.indexer.InvertedIndexInterface import InvertedIndexInterface

class HierarchicalInvertedIndex(InvertedIndexInterface):
    def __init__(self, base_path: str = "datamarts/inverted_index"):
        self.base_path = Path(base_path)

    def initialize(self) -> bool:
        try:
            self.base_path.mkdir(parents=True, exist_ok=True)
            logging.info(f"Initialized hierarchical inverted index at {self.base_path}")
            return True

        except IOError as e:
            logging.error(f"Failed to initialize hierarchical index: {e}")
            return False

    def add_document_to_term(self, term: str, book_id: int) -> bool:
        try:
            term_file = self._get_term_file_path(term)

            # Create subdirectory if it doesn't exist
            term_file.parent.mkdir(parents=True, exist_ok=True)

            # Read existing postings if file exists
            if term_file.exists():
                with open(term_file, 'r', encoding='utf-8') as f:
                    # Each line is a book ID
                    existing_ids = {int(line.strip()) for line in f if line.strip()}
            else:
                existing_ids = set()

            # Add new book_id
            existing_ids.add(book_id)

            # Write back all IDs, sorted for consistency
            with open(term_file, 'w', encoding='utf-8') as f:
                for bid in sorted(existing_ids):
                    f.write(f"{bid}\n")

            return True

        except (IOError, ValueError) as e:
            logging.error(f"Failed to add document {book_id} to term '{term}': {e}")
            return False

    def get_documents_for_term(self, term: str) -> Set[int]:
        try:
            term_file = self._get_term_file_path(term)

            if not term_file.exists():
                return set()

            with open(term_file, 'r', encoding='utf-8') as f:
                return {int(line.strip()) for line in f if line.strip()}

        except (IOError, ValueError) as e:
            logging.error(f"Failed to get documents for term '{term}': {e}")
            return set()

    def get_all_terms(self) -> List[str]:
        try:
            terms = []

            # Walk through all subdirectories
            for subdir in self.base_path.iterdir():
                if subdir.is_dir():
                    # Each .txt file represents a term
                    for term_file in subdir.glob("*.txt"):
                        # Remove the .txt extension to get the term
                        terms.append(term_file.stem)

            return sorted(terms)

        except IOError as e:
            logging.error(f"Failed to get all terms: {e}")
            return []

    def get_index_size(self) -> int:
        return len(self.get_all_terms())

    def close(self) -> None:
        logging.info("Closed hierarchical inverted index (no cleanup needed)")

    def _get_term_file_path(self, term: str) -> Path:
        if not term:
            subdir = "_empty"
        elif term[0].isalpha():
            subdir = term[0].lower()
        else:
            subdir = "_other"

        return self.base_path / subdir / f"{term}.txt"