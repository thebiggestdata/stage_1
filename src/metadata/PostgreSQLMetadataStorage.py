import logging
from typing import List, Optional

try:
    import psycopg2
    from psycopg2 import sql
    from psycopg2.extras import DictCursor

    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False
    logging.warning("psycopg2 not installed. PostgreSQL storage will not work.")

from src.metadata.MetadataStorageInterface import MetadataStorageInterface
from src.metadata.BookMetadata import BookMetadata


class PostgreSQLMetadataStorage(MetadataStorageInterface):
    def __init__(
            self,
            host: str = "localhost",
            port: int = 5432,
            database: str = "gutenberg_search",
            user: str = "postgres",
            password: str = "postgres"
    ):
        if not PSYCOPG2_AVAILABLE:
            raise ImportError(
                "psycopg2 is required for PostgreSQL storage. "
                "Install it with: pip install psycopg2-binary"
            )

        self.connection_params = {
            "host": host,
            "port": port,
            "database": database,
            "user": user,
            "password": password
        }
        self.connection = None

    def initialize(self) -> bool:
        try:
            self.connection = psycopg2.connect(**self.connection_params)
            self.connection.autocommit = False  # Explicit transaction control

            cursor = self.connection.cursor()

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS books (
                    book_id INTEGER PRIMARY KEY,
                    title TEXT,
                    author VARCHAR(500),
                    language VARCHAR(10),
                    release_date VARCHAR(100)
                )
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_books_author 
                ON books(author)
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_books_language 
                ON books(language)
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_books_title_gin 
                ON books USING gin(to_tsvector('english', title))
            """)

            self.connection.commit()
            logging.info(
                f"Initialized PostgreSQL metadata storage: "
                f"{self.connection_params['database']}"
            )
            return True

        except psycopg2.Error as e:
            logging.error(f"Failed to initialize PostgreSQL metadata storage: {e}")
            if self.connection:
                self.connection.rollback()
            return False

    def insert_book_metadata(self, metadata: BookMetadata) -> bool:
        if not self.connection:
            logging.error("Storage not initialized. Call initialize() first.")
            return False

        try:
            cursor = self.connection.cursor()

            cursor.execute("""
                INSERT INTO books (book_id, title, author, language, release_date)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (book_id) 
                DO UPDATE SET
                    title = EXCLUDED.title,
                    author = EXCLUDED.author,
                    language = EXCLUDED.language,
                    release_date = EXCLUDED.release_date
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

        except psycopg2.Error as e:
            logging.error(f"Failed to insert metadata for book {metadata.book_id}: {e}")
            self.connection.rollback()
            return False

    def get_book_by_id(self, book_id: int) -> Optional[BookMetadata]:
        if not self.connection:
            logging.error("Storage not initialized. Call initialize() first.")
            return None

        try:
            cursor = self.connection.cursor(cursor_factory=DictCursor)
            cursor.execute("""
                SELECT book_id, title, author, language, release_date
                FROM books
                WHERE book_id = %s
            """, (book_id,))

            row = cursor.fetchone()
            if row:
                return self._row_to_metadata(row)
            return None

        except psycopg2.Error as e:
            logging.error(f"Failed to get book {book_id}: {e}")
            return None

    def get_books_by_author(self, author: str) -> List[BookMetadata]:
        if not self.connection:
            logging.error("Storage not initialized. Call initialize() first.")
            return []

        try:
            cursor = self.connection.cursor(cursor_factory=DictCursor)
            cursor.execute("""
                SELECT book_id, title, author, language, release_date
                FROM books
                WHERE author ILIKE %s
                ORDER BY title
            """, (f"%{author}%",))

            rows = cursor.fetchall()
            return [self._row_to_metadata(row) for row in rows]

        except psycopg2.Error as e:
            logging.error(f"Failed to get books by author '{author}': {e}")
            return []

    def get_books_by_language(self, language: str) -> List[BookMetadata]:
        if not self.connection:
            logging.error("Storage not initialized. Call initialize() first.")
            return []

        try:
            cursor = self.connection.cursor(cursor_factory=DictCursor)
            cursor.execute("""
                SELECT book_id, title, author, language, release_date
                FROM books
                WHERE language = %s
                ORDER BY title
            """, (language,))

            rows = cursor.fetchall()
            return [self._row_to_metadata(row) for row in rows]

        except psycopg2.Error as e:
            logging.error(f"Failed to get books by language '{language}': {e}")
            return []

    def get_all_books(self, limit: Optional[int] = None) -> List[BookMetadata]:
        if not self.connection:
            logging.error("Storage not initialized. Call initialize() first.")
            return []

        try:
            cursor = self.connection.cursor(cursor_factory=DictCursor)

            if limit:
                cursor.execute("""
                    SELECT book_id, title, author, language, release_date
                    FROM books
                    ORDER BY book_id
                    LIMIT %s
                """, (limit,))
            else:
                cursor.execute("""
                    SELECT book_id, title, author, language, release_date
                    FROM books
                    ORDER BY book_id
                """)

            rows = cursor.fetchall()
            return [self._row_to_metadata(row) for row in rows]

        except psycopg2.Error as e:
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

        except psycopg2.Error as e:
            logging.error(f"Failed to get total books count: {e}")
            return 0

    def book_exists(self, book_id: int) -> bool:
        if not self.connection:
            logging.error("Storage not initialized. Call initialize() first.")
            return False

        try:
            cursor = self.connection.cursor()
            cursor.execute("""
                SELECT EXISTS(SELECT 1 FROM books WHERE book_id = %s)
            """, (book_id,))

            return cursor.fetchone()[0]

        except psycopg2.Error as e:
            logging.error(f"Failed to check if book {book_id} exists: {e}")
            return False

    def close(self) -> None:
        if self.connection:
            self.connection.close()
            logging.info("Closed PostgreSQL metadata storage connection")

    def _row_to_metadata(self, row) -> BookMetadata:
        return BookMetadata(
            book_id=row["book_id"],
            title=row["title"],
            author=row["author"],
            language=row["language"],
            release_date=row["release_date"]
        )