import logging
from pathlib import Path
from typing import Optional

from src.indexer.InvertedIndexInterface import InvertedIndexInterface
from src.indexer.TextProcessor import TextProcessor


class BookIndexer:
    def __init__(self, inverted_index: InvertedIndexInterface,
                 text_processor: TextProcessor = None,
                 datalake_base_path: str = "datalake/books"):
        self.inverted_index = inverted_index
        self.text_processor = text_processor if text_processor else TextProcessor()
        self.datalake_base_path = Path(datalake_base_path)

        logging.info("BookIndexer initialized")

    def index_book(self, book_id: int, download_date: str, download_hour: str) -> bool:
        try:
            book_text = self._read_book_body(book_id, download_date, download_hour)

            if book_text is None:
                logging.error(f"Failed to read book {book_id}, skipping indexing")
                return False

            processed_terms = self.text_processor.process(book_text)

            logging.info(
                f"Book {book_id}: extracted {len(processed_terms)} tokens "
                f"(after stopword filtering)"
            )

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

            return successful_updates > 0

        except Exception as e:
            logging.error(f"Unexpected error indexing book {book_id}: {e}")
            return False

    def _read_book_body(self, book_id: int, download_date: str, download_hour: str) -> Optional[str]:
        try:
            body_path = (
                    self.datalake_base_path /
                    download_date /
                    download_hour /
                    f"{book_id}.body.txt"
            )

            if not body_path.exists():
                logging.error(f"Body file not found: {body_path}")
                return None

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
        return {
            'total_unique_terms': self.inverted_index.get_index_size(),
            'index_type': type(self.inverted_index).__name__
        }