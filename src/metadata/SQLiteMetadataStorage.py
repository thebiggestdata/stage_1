import logging
import sqlite3
from pathlib import Path
from typing import List, Optional
from src.metadata.MetadataStorageInterface import MetadataStorageInterface
from src.metadata.BookMetadata import BookMetadata


class SQLiteMetadataStorage(MetadataStorageInterface):
    def __init__(self, db_path: str = "datamarts/metadata.db"):
        self.db_path = Path(db_path)
        self.connection = None

    def initialize(self) -> bool:
        try:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            self.connection = sqlite3.connect(str(self.db_path))
            cursor = self.connection.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS books (
                    book_id INTEGER PRIMARY KEY,
                    title TEXT,
                    author TEXT,
                    language TEXT,
                    release_date TEXT
                )
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_author 
                ON books(author)
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_language 
                ON books(language)
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_title 
                ON books(title)
            """)

            self.connection.commit()
            logging.info(f"Initialized SQLite metadata storage at {self.db_path}")
            return True
        except sqlite3.Error as e:
            logging.error(f"Failed to initialize SQLite metadata storage: {e}")
            return False

    def insert_book_metadata(self, metadata: BookMetadata) -> bool:
        if not self.connection:
            logging.error("Storage not initialized. Call initialize() first.")
            return False
        try:
            cursor = self.connection.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO books 
                (book_id, title, author, language, release_date)
                VALUES (?, ?, ?, ?, ?)
            """, (
                metadata.book_id,
                metadata.title,
                metadata.author,
                metadata.language,
                metadata.release_date
            ))
            self.connection.commit()
            logging.debug(f"Inserted/updated metadata for book {metadata.book_id}")
            return True

        except sqlite3.Error as e:
            logging.error(f"Failed to insert metadata for book {metadata.book_id}: {e}")
            return False

    def get_book_by_id(self, book_id: int) -> Optional[BookMetadata]:
        if not self.connection:
            logging.error("Storage not initialized. Call initialize() first.")
            return None
        try:
            cursor = self.connection.cursor()
            cursor.execute("""
                SELECT book_id, title, author, language, release_date
                FROM books
                WHERE book_id = ?
            """, (book_id,))
            row = cursor.fetchone()
            if row:
                return self._row_to_metadata(row)
            return None
        except sqlite3.Error as e:
            logging.error(f"Failed to get book {book_id}: {e}")
            return None

    def get_books_by_author(self, author: str) -> List[BookMetadata]:
        if not self.connection:
            logging.error("Storage not initialized. Call initialize() first.")
            return []

        try:
            cursor = self.connection.cursor()
            cursor.execute("""
                SELECT book_id, title, author, language, release_date
                FROM books
                WHERE author LIKE ?
                ORDER BY title
            """, (f"%{author}%",))

            rows = cursor.fetchall()
            return [self._row_to_metadata(row) for row in rows]

        except sqlite3.Error as e:
            logging.error(f"Failed to get books by author '{author}': {e}")
            return []

    def get_books_by_language(self, language: str) -> List[BookMetadata]:
        if not self.connection:
            logging.error("Storage not initialized. Call initialize() first.")
            return []

        try:
            cursor = self.connection.cursor()
            cursor.execute("""
                SELECT book_id, title, author, language, release_date
                FROM books
                WHERE language = ?
                ORDER BY title
            """, (language,))

            rows = cursor.fetchall()
            return [self._row_to_metadata(row) for row in rows]

        except sqlite3.Error as e:
            logging.error(f"Failed to get books by language '{language}': {e}")
            return []

    def get_all_books(self, limit: Optional[int] = None) -> List[BookMetadata]:
        if not self.connection:
            logging.error("Storage not initialized. Call initialize() first.")
            return []

        try:
            cursor = self.connection.cursor()

            if limit:
                cursor.execute("""
                    SELECT book_id, title, author, language, release_date
                    FROM books
                    ORDER BY book_id
                    LIMIT ?
                """, (limit,))
            else:
                cursor.execute("""
                    SELECT book_id, title, author, language, release_date
                    FROM books
                    ORDER BY book_id
                """)

            rows = cursor.fetchall()
            return [self._row_to_metadata(row) for row in rows]

        except sqlite3.Error as e:
            logging.error(f"Failed to get all books: {e}")
            return []

    def get_total_books(self) -> int:
        if not self.connection:
            logging.error("Storage not initialized. Call initialize() first.")
            return 0

        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT COUNT(*) FROM books")
            return cursor.fetchone()[0]

        except sqlite3.Error as e:
            logging.error(f"Failed to get total books count: {e}")
            return 0

    def book_exists(self, book_id: int) -> bool:
        if not self.connection:
            logging.error("Storage not initialized. Call initialize() first.")
            return False

        try:
            cursor = self.connection.cursor()
            cursor.execute("""
                SELECT 1 FROM books WHERE book_id = ? LIMIT 1
            """, (book_id,))

            return cursor.fetchone() is not None

        except sqlite3.Error as e:
            logging.error(f"Failed to check if book {book_id} exists: {e}")
            return False

    def close(self) -> None:
        if self.connection:
            self.connection.close()
            logging.info("Closed SQLite metadata storage connection")

    @staticmethod
    def _row_to_metadata(row: tuple) -> BookMetadata:
        return BookMetadata(
            book_id=row[0],
            title=row[1],
            author=row[2],
            language=row[3],
            release_date=row[4]
        )