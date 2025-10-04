import logging
from pathlib import Path
from typing import Optional

from src.indexer.InvertedIndexInterface import InvertedIndexInterface
from src.indexer.TextProcessor import TextProcessor


class BookIndexer:
    """
    Orchestrates the indexing of books into the inverted index.

    This class is responsible for:
    1. Reading book content from the datalake
    2. Processing the text through the complete pipeline
    3. Updating the inverted index with the processed terms

    It follows the Single Responsibility Principle by delegating:
    - Text processing to TextProcessor
    - Index storage to InvertedIndexInterface implementations
    """

    def __init__(
            self,
            inverted_index: InvertedIndexInterface,
            text_processor: TextProcessor = None,
            datalake_base_path: str = "datalake/books"
    ):
        """
        Initialize the book indexer with its dependencies.

        Args:
            inverted_index: The index implementation to use (SQLite, JSON, or Hierarchical)
            text_processor: The text processor to use (creates default if None)
            datalake_base_path: Base path where book files are stored
        """
        self.inverted_index = inverted_index
        self.text_processor = text_processor if text_processor else TextProcessor()
        self.datalake_base_path = Path(datalake_base_path)

        logging.info("BookIndexer initialized")

    def index_book(self, book_id: int, download_date: str, download_hour: str) -> bool:
        """
        Indexes a single book by reading its body file and processing all terms.

        This is the main entry point that executes the complete indexing pipeline:
        1. Locate and read the book's body.txt file
        2. Process the text (tokenize + filter stopwords)
        3. Update the inverted index for each processed term

        Args:
            book_id: The ID of the book to index
            download_date: Date when the book was downloaded (YYYYMMDD format)
            download_hour: Hour when the book was downloaded (HH format, 24-hour)

        Returns:
            True if indexing was successful, False otherwise
        """
        try:
            # Step 1: Read the book content from the datalake
            book_text = self._read_book_body(book_id, download_date, download_hour)

            if book_text is None:
                logging.error(f"Failed to read book {book_id}, skipping indexing")
                return False

            # Step 2: Process the text to get clean, searchable terms
            # This handles tokenization and stopword filtering
            processed_terms = self.text_processor.process(book_text)

            logging.info(
                f"Book {book_id}: extracted {len(processed_terms)} tokens "
                f"(after stopword filtering)"
            )

            # Step 3: Update the inverted index for each unique term
            # We use a set to avoid adding the same book_id multiple times for a term
            unique_terms = set(processed_terms)

            successful_updates = 0
            for term in unique_terms:
                if self.inverted_index.add_document_to_term(term, book_id):
                    successful_updates += 1
                else:
                    logging.warning(f"Failed to add term '{term}' for book {book_id}")

            logging.info(
                f"Book {book_id}: successfully indexed {successful_updates} "
                f"unique terms out of {len(unique_terms)}"
            )

            # Consider it successful if we indexed at least some terms
            return successful_updates > 0

        except Exception as e:
            logging.error(f"Unexpected error indexing book {book_id}: {e}")
            return False

    def _read_book_body(self, book_id: int, download_date: str, download_hour: str) -> Optional[str]:
        """
        Reads the body.txt file for a specific book from the datalake.

        The datalake structure is:
        datalake/YYYYMMDD/HH/<BOOK_ID>.body.txt

        Args:
            book_id: The ID of the book
            download_date: Date when the book was downloaded (YYYYMMDD format)
            download_hour: Hour when the book was downloaded (HH format, 24-hour)

        Returns:
            The text content of the book, or None if reading failed
        """
        try:
            # Construct the path to the body file
            body_path = (
                    self.datalake_base_path /
                    download_date /
                    download_hour /
                    f"{book_id}.body.txt"
            )

            if not body_path.exists():
                logging.error(f"Body file not found: {body_path}")
                return None

            # Read the entire file content
            with open(body_path, 'r', encoding='utf-8') as f:
                content = f.read()

            if not content.strip():
                logging.warning(f"Body file is empty for book {book_id}")
                return None

            logging.debug(
                f"Successfully read {len(content)} characters from book {book_id}"
            )

            return content

        except IOError as e:
            logging.error(f"Failed to read body file for book {book_id}: {e}")
            return None
        except UnicodeDecodeError as e:
            logging.error(
                f"Encoding error reading book {book_id}. "
                f"File may not be UTF-8: {e}"
            )
            return None

    def get_index_statistics(self) -> dict:
        """
        Returns statistics about the current state of the inverted index.

        Useful for monitoring progress and debugging.

        Returns:
            Dictionary with index statistics
        """
        return {
            'total_unique_terms': self.inverted_index.get_index_size(),
            'index_type': type(self.inverted_index).__name__
        }