import logging
import sys
from argparse import ArgumentParser
from typing import Optional

from crawler.BookFetcher import BookFetcher
from crawler.BookParser import BookParser
from crawler.BookStorage import BookStorage
from crawler.DatalakePathBuilder import DatalakePathBuilder


class Crawler:
    def __init__(self, start_id: int = 1, end_id: int = 1000, delay: float = 1.0):
        self.start_id = start_id
        self.end_id = end_id
        self.delay = delay
        self.current_id = start_id

        self.fetcher = BookFetcher()
        self.parser = BookParser()
        self.path_builder = DatalakePathBuilder()
        self.storage = BookStorage(self.path_builder)

    def download_book(self, book_id: int) -> tuple[bool, Optional[str], Optional[str]]:
        raw_text = self.fetcher.fetch(book_id)
        if not raw_text:
            return False, None, None

        book_content = self.parser.parse(book_id, raw_text)
        if not book_content:
            return False, None, None

        # El storage.save ahora devuelve (success, date, hour)
        return self.storage.save(book_content)

    def download_next_book(self) -> tuple[bool, Optional[int], Optional[str], Optional[str]]:
        max_attempts = 100

        for _ in range(max_attempts):
            if self.current_id > self.end_id:
                logging.warning(f"Reached end of book range ({self.end_id})")
                return False, None, None, None

            book_id = self.current_id
            self.current_id += 1

            success, date_str, hour_str = self.download_book(book_id)

            if success:
                return True, book_id, date_str, hour_str
            else:
                logging.debug(f"Book {book_id} not available, trying next")
                continue

        logging.error(f"Failed to download any book after {max_attempts} attempts")
        return False, None, None, None

    def set_current_id(self, book_id: int):
        self.current_id = book_id

    def crawl_range(self, start_id: Optional[int] = None, end_id: Optional[int] = None):
        start = start_id or self.start_id
        end = end_id or self.end_id

        total = end - start + 1
        successful = 0

        for book_id in range(start, end + 1):
            logging.info(f"Processing book {book_id}")
            success, _, _ = self.download_book(book_id)
            if success:
                successful += 1

        logging.info(f"Downloaded {successful}/{total} books successfully")


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )


def parse_args():
    parser = ArgumentParser(description="Simple Project Gutenberg Crawler")

    parser.add_argument('--start-id', type=int, default=1,
                        help='First book ID to download')
    parser.add_argument('--end-id', type=int, default=1000,
                        help='Last book ID to download')
    parser.add_argument('--delay', type=float, default=1.0,
                        help='Delay between downloads in seconds')

    return parser.parse_args()


def main():
    setup_logging()
    args = parse_args()

    crawler = Crawler(
        start_id=args.start_id,
        end_id=args.end_id,
        delay=args.delay
    )

    crawler.crawl_range()


if __name__ == "__main__":
    main()