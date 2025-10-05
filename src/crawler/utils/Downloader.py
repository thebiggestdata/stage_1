from typing import Optional
import logging


class Downloader:

    def __init__(self, crawler):
        self.crawler = crawler

    def download_book(self, book_id: int, crawler) -> tuple[bool, Optional[str], Optional[str]]:
        raw_text = crawler.fetcher.fetch(book_id)
        if not raw_text:
            return False, None, None

        book_content = crawler.parser.parse(book_id, raw_text)
        if not book_content:
            return False, None, None

        result = crawler.storage.save(book_content)
        if isinstance(result, tuple):
            return result
        elif result:
            return True, None, None
        else:
            return False, None, None

    def download_next_book(self, crawler) -> tuple[bool, Optional[int], Optional[str], Optional[str]]:
        max_attempts = 100

        for _ in range(max_attempts):
            if crawler.current_id > crawler.end_id:
                logging.warning(f"Reached end of book range ({crawler.end_id})")
                return False, None, None, None

            book_id = crawler.current_id
            crawler.current_id += 1
            success, date_str, hour_str = self.download_book(book_id)

            if success:
                return True, book_id, date_str, hour_str
            else:
                logging.debug(f"Book {book_id} not available, trying next")
                continue

        logging.error(f"Failed to download any book after {max_attempts} attempts")
        return False, None, None, None