import logging
from pathlib import Path
from typing import Set
from tqdm import tqdm

from metadata.MongoDBMetadataStorage import MongoDBMetadataStorage
from src.crawler.Crawler import Crawler
from src.indexer.BookIndexer import BookIndexer
from src.metadata.MetadataExtractor import MetadataExtractor
from src.indexer.MongoDBInvertedIndex import MongoDBInvertedIndex


class ControlLayer:
    def __init__(self, control_path: str = "control", datalake_path: str = "datalake", max_book_id: int = 70000):
        self.control_path = Path(control_path)
        self.max_book_id = max_book_id
        self.downloads_file = self.control_path / "downloaded_books.txt"
        self.processed_file = self.control_path / "processed_books.txt"
        self.last_downloaded_id_file = self.control_path / "last_downloaded_id.txt"
        self.last_processed_id_file = self.control_path / "last_processed_id.txt"

        self.crawler = Crawler(start_id=1, end_id=max_book_id)
        last_downloaded = self._get_last_id(self.last_downloaded_id_file)
        if last_downloaded:
            self.crawler.set_current_id(last_downloaded + 1)
            logging.info(f"Resuming downloads from book ID {last_downloaded + 1}")

        self._initialize_storage(datalake_path)

        self.control_path.mkdir(parents=True, exist_ok=True)
        logging.info("Control Layer initialized successfully")

    def _initialize_storage(self, datalake_path: str):
        self.inverted_index = MongoDBInvertedIndex()
        if not self.inverted_index.initialize():
            raise RuntimeError("Failed to initialize inverted index")

        self.metadata_storage = MongoDBMetadataStorage()
        if not self.metadata_storage.initialize():
            raise RuntimeError("Failed to initialize metadata storage")

        self.book_indexer = BookIndexer(
            inverted_index=self.inverted_index,
            datalake_base_path=datalake_path
        )
        self.metadata_extractor = MetadataExtractor(
            metadata_storage=self.metadata_storage,
            datalake_base_path=datalake_path
        )

    def run_pipeline(self, total_books: int = 20):
        logging.info(f"{'=' * 60}\nStarting pipeline for {total_books} books\n{'=' * 60}")

        self._download_phase(total_books)
        self._processing_phase()

        logging.info(f"\n{'=' * 60}\nPipeline execution completed\n{'=' * 60}")
        self._show_final_statistics()

    def _download_phase(self, total_books: int):
        downloaded = self._load_book_set(self.downloads_file)
        books_needed = total_books - len(downloaded)

        if books_needed <= 0:
            logging.info(f"Already have {len(downloaded)} books downloaded. Skipping download phase.")
            return

        logging.info(f"\n{'=' * 60}\nDOWNLOAD PHASE: Downloading {books_needed} more books\n{'=' * 60}")

        with tqdm(total=books_needed, desc="Downloading books", unit="book") as pbar:
            downloaded_count = 0
            while downloaded_count < books_needed:
                success, book_id, download_date, download_hour = self.crawler.download_next_book()

                if success:
                    book_entry = f"{book_id}|{download_date}|{download_hour}"
                    self._append_to_file(self.downloads_file, book_entry)
                    self._save_last_id(self.last_downloaded_id_file, book_id)
                    downloaded_count += 1
                    pbar.update(1)
                    pbar.set_postfix({"last_book_id": book_id})

    def _processing_phase(self):
        ready_to_process = self._load_book_set(self.downloads_file) - self._load_book_set(self.processed_file)

        if not ready_to_process:
            logging.info("No books to process. All downloaded books are already processed.")
            return

        logging.info(f"\n{'=' * 60}\nPROCESSING PHASE: Processing {len(ready_to_process)} books\n{'=' * 60}")

        for book_entry in tqdm(ready_to_process, desc="Processing books", unit="book"):
            self._process_single_book(book_entry)

    def _process_single_book(self, book_entry: str):
        book_id, download_date, download_hour = book_entry.split('|')
        book_id = int(book_id)

        metadata_success = self.metadata_extractor.extract_and_store_metadata(
            book_id, download_date, download_hour
        )

        if not metadata_success:
            logging.error(f"Failed to extract metadata for book {book_id}")
            return

        indexing_success = self.book_indexer.index_book(
            book_id, download_date, download_hour
        )

        if not indexing_success:
            logging.error(f"Failed to index book {book_id}")
            return

        self._append_to_file(self.processed_file, book_entry)
        self._save_last_id(self.last_processed_id_file, book_id)

    def _get_last_id(self, file_path: Path) -> int:
        if file_path.exists():
            try:
                return int(file_path.read_text(encoding='utf-8').strip())
            except (ValueError, IOError) as e:
                logging.warning(f"Error reading last ID from {file_path.name}: {e}")
        return 0

    def _save_last_id(self, file_path: Path, book_id: int):
        try:
            file_path.write_text(str(book_id), encoding='utf-8')
        except IOError as e:
            logging.error(f"Failed to save last ID to {file_path.name}: {e}")

    def _append_to_file(self, file_path: Path, content: str):
        with open(file_path, 'a', encoding='utf-8') as f:
            f.write(f"{content}\n")

    @staticmethod
    def _load_book_set(file_path: Path) -> Set[str]:
        return {line.strip() for line in file_path.read_text(encoding='utf-8').splitlines() if
                line.strip()} if file_path.exists() else set()

    def _show_final_statistics(self):
        downloaded = self._load_book_set(self.downloads_file)
        processed = self._load_book_set(self.processed_file)
        index_stats = self.book_indexer.get_index_statistics()
        metadata_stats = self.metadata_extractor.get_storage_statistics()

        stats_message = f"""
Final Statistics:
  Total books downloaded: {len(downloaded)}
  Total books processed: {len(processed)}
  Books pending: {len(downloaded - processed)}

Inverted Index:
  Unique terms: {index_stats['total_unique_terms']}
  Index type: {index_stats['index_type']}

Metadata Storage:
  Total books: {metadata_stats['total_books']}
  Storage type: {metadata_stats['storage_type']}
"""
        logging.info(stats_message)

    def close(self):
        self.inverted_index.close()
        self.metadata_storage.close()
        logging.info("Control Layer shut down successfully")


def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    print(f"{'=' * 60}\nSTAGE 1: Building the Data Layer\nSearch Engine Project - Big Data\n{'=' * 60}\n")

    control = ControlLayer()
    try:
        control.run_pipeline(total_books=100)
    except KeyboardInterrupt:
        print("\n\nPipeline interrupted by user")
    except Exception as e:
        logging.error(f"Pipeline failed: {e}", exc_info=True)
        raise
    finally:
        control.close()

    print("\nPipeline execution finished.")


if __name__ == "__main__":
    main()