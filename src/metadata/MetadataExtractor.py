import logging
from pathlib import Path
from typing import Optional
from src.metadata.MetadataStorageInterface import MetadataStorageInterface
from src.metadata.HeaderParser import HeaderParser


class MetadataExtractor:
    def __init__(
            self,
            metadata_storage: MetadataStorageInterface,
            header_parser: HeaderParser = None,
            datalake_base_path: str = "datalake"
    ):
        self.metadata_storage = metadata_storage
        self.header_parser = header_parser if header_parser else HeaderParser()
        self.datalake_base_path = Path(datalake_base_path)
        logging.info("MetadataExtractor initialized")

    def extract_and_store_metadata(self, book_id: int, download_date: str, download_hour: str) -> bool:
        try:
            header_text = self._read_book_header(book_id, download_date, download_hour)

            if header_text is None:
                logging.error(f"Failed to read header for book {book_id}, skipping metadata extraction")
                return False
            metadata = self.header_parser.parse(book_id, header_text)

            if not metadata.is_complete():
                logging.warning(
                    f"Book {book_id}: extracted incomplete metadata - "
                    f"some essential fields are missing"
                )
            logging.info(
                f"Book {book_id}: extracted metadata - "
                f"Title: '{metadata.title}', Author: '{metadata.author}'"
            )
            success = self.metadata_storage.insert_book_metadata(metadata)
            if success:
                logging.info(f"Book {book_id}: successfully stored metadata in database")
            else:
                logging.error(f"Book {book_id}: failed to store metadata in database")
            return success

        except Exception as e:
            logging.error(f"Unexpected error extracting metadata for book {book_id}: {e}")
            return False

    def _read_book_header(self, book_id: int, download_date: str, download_hour: str) -> Optional[str]:
        try:
            header_path = (
                    self.datalake_base_path /
                    download_date /
                    download_hour /
                    f"{book_id}.header.txt"
            )
            if not header_path.exists():
                logging.error(f"Header file not found: {header_path}")
                return None
            with open(header_path, 'r', encoding='utf-8') as f:
                content = f.read()
            if not content.strip():
                logging.warning(f"Header file is empty for book {book_id}")
                return None
            logging.debug(f"Successfully read {len(content)} characters from header of book {book_id}")
            return content
        except IOError as e:
            logging.error(f"Failed to read header file for book {book_id}: {e}")
            return None
        except UnicodeDecodeError as e:
            logging.error(
                f"Encoding error reading header for book {book_id}. "
                f"File may not be UTF-8: {e}"
            )
            return None

    def get_storage_statistics(self) -> dict:
        return {
            'total_books': self.metadata_storage.get_total_books(),
            'storage_type': type(self.metadata_storage).__name__
        }