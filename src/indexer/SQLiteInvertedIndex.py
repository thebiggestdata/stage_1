import json
import logging
import sqlite3
from pathlib import Path
from typing import List, Set

from src.indexer.InvertedIndexInterface import InvertedIndexInterface


class SQLiteInvertedIndex(InvertedIndexInterface):
    """
    SQLite-based implementation of the inverted index.

    This is the recommended approach for most use cases because it provides:
    - Automatic indexing for fast lookups
    - ACID transactions (no data loss if the program crashes)
    - Good performance without external dependencies
    - Simple single-file storage

    The posting lists are stored as JSON strings inside the database.
    While this isn't the most space-efficient approach, it keeps the
    implementation simple and performs well for this project's scale.
    """

    def __init__(self, db_path: str = "datamarts/inverted_index.db"):
        """
        Initialize the SQLite index.

        Args:
            db_path: Path where the SQLite database file will be stored
        """
        self.db_path = Path(db_path)
        self.connection = None

    def initialize(self) -> bool:
        """
        Creates the database file and table structure.

        The table has two columns:
        - term: the indexed word (PRIMARY KEY for fast lookups)
        - postings: JSON array of book IDs as a string

        Returns:
            True if successful, False otherwise
        """
        try:
            # Ensure the parent directory exists
            self.db_path.parent.mkdir(parents=True, exist_ok=True)

            # Connect to the database (creates file if it doesn't exist)
            self.connection = sqlite3.connect(str(self.db_path))
            cursor = self.connection.cursor()

            # Create the table if it doesn't exist
            # Using term as PRIMARY KEY automatically creates an index on it
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS inverted_index (
                    term TEXT PRIMARY KEY,
                    postings TEXT NOT NULL
                )
            """)

            self.connection.commit()
            logging.info(f"Initialized SQLite inverted index at {self.db_path}")
            return True

        except sqlite3.Error as e:
            logging.error(f"Failed to initialize SQLite index: {e}")
            return False

    def add_document_to_term(self, term: str, book_id: int) -> bool:
        """
        Adds a book ID to the posting list of a term.

        This implementation uses an "upsert" pattern:
        - If the term exists: fetch its posting list, add the new book_id, update
        - If the term doesn't exist: create a new entry with a single-element posting list

        Args:
            term: The word to index
            book_id: The book containing this word

        Returns:
            True if successful, False otherwise
        """
        if not self.connection:
            logging.error("Index not initialized. Call initialize() first.")
            return False

        try:
            cursor = self.connection.cursor()

            # Try to fetch the existing posting list for this term
            cursor.execute("SELECT postings FROM inverted_index WHERE term = ?", (term,))
            result = cursor.fetchone()

            if result:
                # Term exists - update its posting list
                postings_set = set(json.loads(result[0]))
                postings_set.add(book_id)
                postings_json = json.dumps(sorted(list(postings_set)))

                cursor.execute(
                    "UPDATE inverted_index SET postings = ? WHERE term = ?",
                    (postings_json, term)
                )
            else:
                # Term doesn't exist - create new entry
                postings_json = json.dumps([book_id])
                cursor.execute(
                    "INSERT INTO inverted_index (term, postings) VALUES (?, ?)",
                    (term, postings_json)
                )

            self.connection.commit()
            return True

        except sqlite3.Error as e:
            logging.error(f"Failed to add document {book_id} to term '{term}': {e}")
            return False

    def get_documents_for_term(self, term: str) -> Set[int]:
        """
        Retrieves all book IDs containing a specific term.

        Args:
            term: The term to search for

        Returns:
            Set of book IDs, empty set if term not found
        """
        if not self.connection:
            logging.error("Index not initialized. Call initialize() first.")
            return set()

        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT postings FROM inverted_index WHERE term = ?", (term,))
            result = cursor.fetchone()

            if result:
                return set(json.loads(result[0]))
            return set()

        except sqlite3.Error as e:
            logging.error(f"Failed to get documents for term '{term}': {e}")
            return set()

    def get_all_terms(self) -> List[str]:
        """
        Returns all terms in the index.

        Returns:
            List of all indexed terms, sorted alphabetically
        """
        if not self.connection:
            logging.error("Index not initialized. Call initialize() first.")
            return []

        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT term FROM inverted_index ORDER BY term")
            return [row[0] for row in cursor.fetchall()]

        except sqlite3.Error as e:
            logging.error(f"Failed to get all terms: {e}")
            return []

    def get_index_size(self) -> int:
        """
        Returns the number of unique terms in the index.

        Returns:
            Number of terms
        """
        if not self.connection:
            logging.error("Index not initialized. Call initialize() first.")
            return 0

        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT COUNT(*) FROM inverted_index")
            return cursor.fetchone()[0]

        except sqlite3.Error as e:
            logging.error(f"Failed to get index size: {e}")
            return 0

    def close(self) -> None:
        """
        Closes the database connection.
        Always call this when you're done using the index.
        """
        if self.connection:
            self.connection.close()
            logging.info("Closed SQLite inverted index connection")
