import logging
from typing import Optional
import requests


class BookFetcher:
    BASE_URL = "https://www.gutenberg.org/cache/epub/{book_id}/pg{book_id}.txt"

    def fetch(self, book_id: int) -> Optional[str]:
        url = self.BASE_URL.format(book_id=book_id)
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            logging.info(f"Successfully fetched book {book_id}")
            return response.text
        except requests.RequestException as e:
            logging.error(f"Failed to fetch book {book_id}: {e}")
            return None