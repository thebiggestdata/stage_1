from indexer.SQLiteInvertedIndex import SQLiteInvertedIndex
from indexer.HierarchicalInvertedIndex import HierarchicalInvertedIndex
from indexer.MongoDBInvertedIndex import MongoDBInvertedIndex
from metadata.SQLiteMetadataStorage import SQLiteMetadataStorage
from metadata.MongoDBMetadataStorage import MongoDBMetadataStorage


class BasicQueryEngine:
    def __init__(self, index_type="sqlite", metadata_type="sqlite"):
        if index_type == "sqlite":
            self.index = SQLiteInvertedIndex("datamarts/inverted_index.db")
        elif index_type == "hierarchical":
            self.index = HierarchicalInvertedIndex("datamarts/inverted_index/")
        elif index_type == "mongodb":
            self.index = MongoDBInvertedIndex()
        else:
            raise ValueError("Invalid index_type. Use: sqlite | hierarchical | mongodb")

        if not self.index.initialize():
            raise RuntimeError(f"Failed to initialize {index_type} index")

        if metadata_type == "sqlite":
            self.metadata_storage = SQLiteMetadataStorage()
        elif metadata_type == "mongodb":
            self.metadata_storage = MongoDBMetadataStorage()
        else:
            raise ValueError("Invalid metadata_type. Use: sqlite | mongodb | postgresql")

        if not self.metadata_storage.initialize():
            raise RuntimeError(f"Failed to initialize {metadata_type} metadata storage")

        self.index_type = index_type
        self.metadata_type = metadata_type
        print(f"Query engine initialized with {index_type} index and {metadata_type} metadata")

    def search_by_term(self, term):
        results = self.index.get_documents_for_term(term.lower())

        if not results:
            print(f"\nNo results found for '{term}'")
        else:
            print(f"\nResults for '{term}':")
            print(f"  Found in {len(results)} book(s): {sorted(results)}")
        return results

    def search_by_book_id(self, book_id):
        metadata = self.metadata_storage.get_book_by_id(book_id)
        if not metadata:
            print(f"\nNo metadata found for book ID {book_id}")
            return None
        print(f"\nBook ID: {book_id}")
        print(f"  Title: {metadata.title}")
        print(f"  Author: {metadata.author}")
        print(f"  Language: {metadata.language}")
        return metadata

    def close(self):
        self.index.close()
        self.metadata_storage.close()

def interactive_search():
    print("=" * 60)
    print("SEARCH ENGINE")
    print("=" * 60)
    print()

    try:
        engine = BasicQueryEngine(
            index_type="mongodb",
            metadata_type="mongodb"
        )
        print("=" * 60)
        print("Ready to search!")
        print("  - Enter a WORD to search in book contents")
        print("  - Enter a NUMBER to get book metadata")
        print("  - Type 'quit' or 'exit' to stop")
        print("=" * 60)
        while True:
            print()
            query = input("Search: ").strip()
            if query.lower() in ['quit', 'exit', 'q']:
                break
            if not query:
                print("Please enter a search term or book ID")
                continue
            if query.isdigit():
                book_id = int(query)
                engine.search_by_book_id(book_id)
            else:
                engine.search_by_term(query)
        engine.close()
    except FileNotFoundError as e:
        print(f"\nError: Index file not found")
        print(f"   {e}")
        print("   Make sure you've run the pipeline first (ControlLayer.py)")
    except Exception as e:
        print(f"\nError: {e}")
    print("\nGoodbye!")

def quick_search_term(term, index_type="mongodb", metadata_type="mongodb"):
    engine = BasicQueryEngine(index_type=index_type, metadata_type=metadata_type)
    results = engine.search_by_term(term)
    engine.close()
    return results

def quick_search_book(book_id, index_type="mongodb", metadata_type="mongodb"):
    engine = BasicQueryEngine(index_type=index_type, metadata_type=metadata_type)
    metadata = engine.search_by_book_id(book_id)
    engine.close()
    return metadata

def main():
    import sys

    if len(sys.argv) > 1:
        query = " ".join(sys.argv[1:])
        if query.isdigit():
            print(f"Searching for book ID: {query}")
            quick_search_book(int(query))
        else:
            print(f"Searching for term: '{query}'")
            quick_search_term(query)
    else:
        interactive_search()


if __name__ == "__main__":
    main()