import re
import logging
from typing import Optional
from src.metadata.BookMetadata import BookMetadata


class HeaderParser:
    def __init__(self):
        self.title_pattern = re.compile(r'^title:\s*(.+)$', re.IGNORECASE | re.MULTILINE)
        self.author_pattern = re.compile(r'^author:\s*(.+)$', re.IGNORECASE | re.MULTILINE)
        self.language_pattern = re.compile(r'^language:\s*(.+)$', re.IGNORECASE | re.MULTILINE)
        self.release_date_pattern = re.compile(
            r'^release date:\s*(.+)$',
            re.IGNORECASE | re.MULTILINE
        )

    def parse(self, book_id: int, header_text: str) -> BookMetadata:
        if not header_text or not header_text.strip():
            logging.warning(f"Empty header text for book {book_id}")
            return BookMetadata(book_id=book_id)
        title = self._extract_field(self.title_pattern, header_text)
        author = self._extract_field(self.author_pattern, header_text)
        language = self._extract_field(self.language_pattern, header_text)
        release_date = self._extract_field(self.release_date_pattern, header_text)
        if language:
            language = self._normalize_language(language)
        metadata = BookMetadata(
            book_id=book_id,
            title=title,
            author=author,
            language=language,
            release_date=release_date
        )
        if not metadata.is_complete():
            missing_fields = []
            if not title:
                missing_fields.append("title")
            if not author:
                missing_fields.append("author")
            logging.warning(
                f"Book {book_id}: missing essential fields: {', '.join(missing_fields)}"
            )
        logging.debug(f"Parsed metadata for book {book_id}: {metadata}")
        return metadata


    @staticmethod
    def _extract_field(pattern: re.Pattern, text: str) -> Optional[str]:
        match = pattern.search(text)
        if match:
            value = match.group(1).strip()
            value = re.sub(r'[\[\]]+$', '', value).strip()
            return value if value else None
        return None

    @staticmethod
    def _normalize_language(language: str) -> str:
        language_lower = language.lower()
        if len(language_lower) == 2 and language_lower.isalpha():
            return language_lower
        language_map = {
            'english': 'en',
            'spanish': 'es',
            'french': 'fr',
            'german': 'de',
            'italian': 'it',
            'portuguese': 'pt',
            'dutch': 'nl',
            'russian': 'ru',
            'chinese': 'zh',
            'japanese': 'ja',
            'latin': 'la',
            'greek': 'el',
        }
        for full_name, code in language_map.items():
            if full_name in language_lower:
                return code
        if len(language_lower) >= 2:
            return language_lower[:2]
        return language_lower