import datetime
import logging
from pathlib import Path
from typing import Optional

from src.crawler import DatalakePathBuilder
from src.crawler.BookContent import BookContent


class BookStorage:
    def __init__(self, path_builder: DatalakePathBuilder):
        self.path_builder = path_builder

    def save(self, book_content: BookContent, timestamp: Optional[datetime.datetime] = None) -> tuple[
        bool, Optional[str], Optional[str]]:
        """
        Saves book content to the datalake.

        Returns:
            Tuple of (success, date_string, hour_string) where:
            - success is True if save was successful
            - date_string is in format YYYYMMDD
            - hour_string is in format HH
        """
        if timestamp is None:
            timestamp = datetime.datetime.now()

        directory = self.path_builder.get_book_directory(timestamp)

        try:
            directory.mkdir(parents=True, exist_ok=True)

            header_path = directory / f"{book_content.book_id}.header.txt"
            self._write_file(header_path, book_content.header)

            body_path = directory / f"{book_content.book_id}.body.txt"
            self._write_file(body_path, book_content.body)

            # Extract date and hour strings from the timestamp
            date_str = timestamp.strftime("%Y%m%d")
            hour_str = timestamp.strftime("%H")

            logging.info(f"Saved book {book_content.book_id} to {directory}")
            return True, date_str, hour_str

        except IOError as e:
            logging.error(f"Failed to save book {book_content.book_id}: {e}")
            return False, None, None

    def _write_file(self, path: Path, content: str) -> None:
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
