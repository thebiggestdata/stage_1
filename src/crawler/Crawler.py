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

        self.fetcher = BookFetcher()
        self.parser = BookParser()
        self.path_builder = DatalakePathBuilder()
        self.storage = BookStorage(self.path_builder)

    def download_book(self, book_id: int) -> bool:
        raw_text = self.fetcher.fetch(book_id)
        if not raw_text:
            return False

        book_content = self.parser.parse(book_id, raw_text)
        if not book_content:
            return False

        return self.storage.save(book_content)

    def crawl_range(self, start_id: Optional[int] = None, end_id: Optional[int] = None):
        start = start_id or self.start_id
        end = end_id or self.end_id

        total = end - start + 1
        successful = 0

        for book_id in range(start, end + 1):
            logging.info(f"Processing book {book_id}")
            if self.download_book(book_id):
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