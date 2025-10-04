import json
import logging
import sqlite3
from pathlib import Path
from typing import List, Set

from src.indexer.InvertedIndexInterface import InvertedIndexInterface


class SQLiteInvertedIndex(InvertedIndexInterface):
    def __init__(self, db_path: str = "datamarts/inverted_index.db"):
        self.db_path = Path(db_path)
        self.connection = None

    def initialize(self) -> bool:
        try:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)

            self.connection = sqlite3.connect(str(self.db_path))
            cursor = self.connection.cursor()

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
        if not self.connection:
            logging.error("Index not initialized. Call initialize() first.")
            return False

        try:
            cursor = self.connection.cursor()

            cursor.execute("SELECT postings FROM inverted_index WHERE term = ?", (term,))
            result = cursor.fetchone()

            if result:
                postings_set = set(json.loads(result[0]))
                postings_set.add(book_id)
                postings_json = json.dumps(sorted(list(postings_set)))

                cursor.execute(
                    "UPDATE inverted_index SET postings = ? WHERE term = ?",
                    (postings_json, term)
                )
            else:
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
        if self.connection:
            self.connection.close()
            logging.info("Closed SQLite inverted index connection")
