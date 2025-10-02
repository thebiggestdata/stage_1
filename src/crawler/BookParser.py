import logging
from typing import Optional

from crawler.BookContent import BookContent


class BookParser:
    START_MARKER = "*** START OF THE PROJECT GUTENBERG EBOOK"
    END_MARKER = "*** END OF THE PROJECT GUTENBERG EBOOK"

    def parse(self, book_id: int, raw_text: str) -> Optional[BookContent]:
        if not self._has_valid_markers(raw_text):
            logging.warning(f"Book {book_id} doesn't have valid markers")
            return None

        try:
            header, body_and_footer = raw_text.split(self.START_MARKER, 1)
            body, footer = body_and_footer.split(self.END_MARKER, 1)

            return BookContent(
            book_id =book_id,
            header =header.strip(),
            body =body.strip(),
            footer =footer.strip()
            )

        except ValueError as e:
            logging.error(f"Error parsing book {book_id}: {e}")
            return None

    def _has_valid_markers(self, text: str) -> bool:
        return self.START_MARKER in text and self.END_MARKER in text
