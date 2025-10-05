import datetime
import logging
from pathlib import Path
from typing import Optional

from src.crawler.BookContent import BookContent
from src.crawler.DatalakePathBuilder import DatalakePathBuilder


class BookStorage:
    def __init__(self, path_builder: DatalakePathBuilder):
        self.path_builder = path_builder

    def save(self, book_content: BookContent, timestamp: Optional[datetime.datetime] = None) -> bool:
        directory = self.path_builder.get_book_directory(timestamp)

        try:
            directory.mkdir(parents=True, exist_ok=True)

            header_path = directory / f"{book_content.book_id}.header.txt"
            self._write_file(header_path, book_content.header)

            body_path = directory / f"{book_content.book_id}.body.txt"
            self._write_file(body_path, book_content.body)

            logging.info(f"Saved book {book_content.book_id} to {directory}")
            return True

        except IOError as e:
            logging.error(f"Failed to save book {book_content.book_id}: {e}")
            return False

    @staticmethod
    def _write_file(path: Path, content: str) -> None:
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
