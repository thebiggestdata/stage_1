import logging
import random
import time
from pathlib import Path
from typing import Set
from src.crawler.Crawler import Crawler
from src.indexer.BookIndexer import BookIndexer
from src.indexer.HierarchicalInvertedIndex import HierarchicalInvertedIndex


class ControlLayer:

    def __init__(
            self,
            control_path: str = "control",
            datalake_path: str = "datalake",
            max_book_id: int = 70000
    ):
        self.control_path = Path(control_path)
        self.max_book_id = max_book_id
        self.downloads_file = self.control_path / "downloaded_books.txt"
        self.processed_file = self.control_path / "processed_books.txt"
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.crawler = Crawler()
        self.inverted_index = HierarchicalInvertedIndex()

        if not self.inverted_index.initialize():
            raise RuntimeError("Failed to initialize inverted index")

        self.metadata_storage = SQLiteMetadataStorage()

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
        self.control_path.mkdir(parents=True, exist_ok=True)
        logging.info("Control Layer initialized successfully")

    def run_pipeline(self, num_iterations: int = 10):
        logging.info(f"Starting pipeline for {num_iterations} iterations")

        for iteration in range(1, num_iterations + 1):
            logging.info(f"\n{'=' * 60}")
            logging.info(f"ITERATION {iteration}/{num_iterations}")
            logging.info(f"{'=' * 60}")

            self._pipeline_step()
            self._show_statistics()

            time.sleep(2)

        logging.info("\n" + "=" * 60)
        logging.info("Pipeline execution completed")
        logging.info("=" * 60)
        self._show_final_statistics()

    def _pipeline_step(self):
        downloaded = self._load_book_set(self.downloads_file)
        processed = self._load_book_set(self.processed_file)

        ready_to_process = downloaded - processed

        if ready_to_process:
            self._process_book(ready_to_process)
        else:
            self._download_new_book(downloaded)

    def _process_book(self, ready_to_process: Set[str]):
        book_entry = ready_to_process.pop()
        book_id, download_date, download_hour = book_entry.split('|')
        book_id = int(book_id)

        logging.info(f"Processing book {book_id} (downloaded on {download_date} at {download_hour}:00)")

        logging.info(f"  -> Extracting metadata...")
        metadata_success = self.metadata_extractor.extract_and_store_metadata(
            book_id, download_date, download_hour
        )

        if not metadata_success:
            logging.error(f"  -> Failed to extract metadata")
            return

        logging.info(f"  -> Indexing book content...")
        indexing_success = self.book_indexer.index_book(
            book_id, download_date, download_hour
        )

        if not indexing_success:
            logging.error(f"  -> Failed to index book")
            return

        self._mark_as_processed(book_entry)
        logging.info(f"  -> Book {book_id} successfully processed!")

    def _download_new_book(self, already_downloaded: Set[str]):
        logging.info("No unprocessed books found. Attempting to download a new book...")

        downloaded_ids = {entry.split('|')[0] for entry in already_downloaded}

        for attempt in range(10):
            candidate_id = random.randint(1, self.max_book_id)

            if str(candidate_id) not in downloaded_ids:
                logging.info(f"  -> Attempting to download book {candidate_id}...")

                success, download_date, download_hour = self.crawler.download_book(candidate_id)

                if success:
                    book_entry = f"{candidate_id}|{download_date}|{download_hour}"
                    self._mark_as_downloaded(book_entry)
                    logging.info(f"  -> Book {candidate_id} downloaded successfully!")
                    return
                else:
                    logging.warning(f"  -> Failed to download book {candidate_id}, trying another")

        logging.error("Failed to download any new book after 10 attempts")

    @staticmethod
    def _load_book_set(self, file_path: Path) -> Set[str]:
        if not file_path.exists():
            return set()
        with open(file_path, 'r', encoding='utf-8') as f:
            return {line.strip() for line in f if line.strip()}

    def _mark_as_downloaded(self, book_entry: str):
        with open(self.downloads_file, 'a', encoding='utf-8') as f:
            f.write(f"{book_entry}\n")

    def _mark_as_processed(self, book_entry: str):
        with open(self.processed_file, 'a', encoding='utf-8') as f:
            f.write(f"{book_entry}\n")

    def _show_statistics(self):
        downloaded = self._load_book_set(self.downloads_file)
        processed = self._load_book_set(self.processed_file)

        logging.info(f"\nCurrent Statistics:")
        logging.info(f"  Books downloaded: {len(downloaded)}")
        logging.info(f"  Books processed: {len(processed)}")
        logging.info(f"  Books pending: {len(downloaded - processed)}")

    def _show_final_statistics(self):
        downloaded = self._load_book_set(self.downloads_file)
        processed = self._load_book_set(self.processed_file)

        index_stats = self.book_indexer.get_index_statistics()
        metadata_stats = self.metadata_extractor.get_storage_statistics()

        logging.info("\nFinal Statistics:")
        logging.info(f"  Total books downloaded: {len(downloaded)}")
        logging.info(f"  Total books processed: {len(processed)}")
        logging.info(f"  Books pending: {len(downloaded - processed)}")
        logging.info(f"\nInverted Index:")
        logging.info(f"  Unique terms: {index_stats['total_unique_terms']}")
        logging.info(f"  Index type: {index_stats['index_type']}")
        logging.info(f"\nMetadata Storage:")
        logging.info(f"  Total books: {metadata_stats['total_books']}")
        logging.info(f"  Storage type: {metadata_stats['storage_type']}")

    def close(self):
        self.inverted_index.close()
        self.metadata_storage.close()
        logging.info("Control Layer shut down successfully")


def main():
    print("=" * 60)
    print("STAGE 1: Building the Data Layer")
    print("Search Engine Project - Big Data")
    print("=" * 60)
    print()

    control = ControlLayer()

    try:
        control.run_pipeline(num_iterations=20)
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