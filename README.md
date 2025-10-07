# Stage 1: Search Engine - Data Layer

Search engine for Project Gutenberg books with full-text indexing and metadata storage.

## 📋 Description

This project implements the data layer of a search engine that:
- **Downloads** books automatically from Project Gutenberg
- **Indexes** full content using an inverted index
- **Stores** book metadata (title, author, language, date)
- **Enables searches** by keywords in indexed content

## 🏗️ Architecture

```
src/
├── crawler/              # Book downloading from Gutenberg
│   ├── Crawler.py
│   └── utils/
├── indexer/              # Inverted index creation
│   ├── BookIndexer.py
│   ├── MongoDBInvertedIndex.py
│   ├── SQLiteInvertedIndex.py
│   └── Storage/
├── metadata/             # Metadata storage
│   ├── MetadataExtractor.py
│   ├── MongoDBMetadataStorage.py
│   ├── SQLiteMetadataStorage.py
│   └── BookMetadata.py
├── tests/                # Logic to benchmark the code
├── ControlLayer.py       # Main pipeline orchestrator
└── BasicQueryEngine.py   # CLI search interface

datalake/                 # Downloaded books (organized by date)
└── YYYYMMDD/
    └── HH/

control/                  # Pipeline control files
├── downloaded_books.txt
├── processed_books.txt
├── last_downloaded_id.txt
└── last_processed_id.txt
```

## 🔧 Prerequisites

### Software
- **Python 3.11** or higher
- **MongoDB** (local, default port 27017)

### Verify MongoDB installation
```bash
# Windows
services.msc  # Search for "MongoDB" and verify it's "Running"

# Or test connection
mongosh
```

## 📦 Installation

### 1. Clone the repository
```bash
git clone https://github.com/thebiggestdata/stage_1.git
cd stage_1
```

### 2. Create virtual environment (recommended)
```bash
python -m venv venv

# Windows
venv\Scripts\activate

# Linux/Mac
source venv/bin/activate
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

## 🚀 Usage

### Step 1: Run the Pipeline (Download and Index)

```bash
cd src
python ControlLayer.py
```

**Configuration:**
By default downloads and processes **500 books**. To change:

```python
# In ControlLayer.py, line ~195
control.run_pipeline(total_books=500)  # Change this number
```

**Expected output:**
```
============================================================
STAGE 1: Building the Data Layer
Search Engine Project - Big Data
============================================================

Downloading books: 100%|████████████| 500/500 [25:30<00:00,  1.8s/book]
Processing books: 100%|█████████████| 500/500 [18:45<00:00,  2.1s/book]

Final Statistics:
  Total books downloaded: 500
  Total books processed: 500
  Books pending: 0

Inverted Index:
  Unique terms: 45,832
  Index type: MongoDBInvertedIndex

Metadata Storage:
  Total books: 500
  Storage type: MongoDBMetadataStorage
```

### Step 2: Perform Searches

```bash
python BasicQueryEngine.py
```

**CLI Interface:**
```
=== Gutenberg Search Engine ===

Options:
1. Search by keyword
2. View book metadata
3. Exit

> 1

Enter keyword(s) to search: freedom love

Results found: 23 books
- Book ID: 1342 - Pride and Prejudice
- Book ID: 84 - Frankenstein
- Book ID: 1661 - The Adventures of Sherlock Holmes
...

> 2

Enter Book ID: 1342

=== Book Metadata ===
ID: 1342
Title: Pride and Prejudice
Author: Jane Austen
Language: English
Release Date: 1998-06-01
```

## 📊 Available Implementations

The project supports multiple storage backends:

### Inverted Index
- ✅ **MongoDB** (default)
- ⚪ SQLite
- ⚪ Hierarchical (file-based)

### Metadata
- ✅ **MongoDB** (default)
- ⚪ SQLite

**To change implementation:**
```python
# In ControlLayer.py, line ~26
from src.indexer.MongoDBInvertedIndex import MongoDBInvertedIndex
# Change to:
from src.indexer.SQLiteInvertedIndex import SQLiteInvertedIndex
```

## 🧪 Verification

### 1. Verify downloaded books
```bash
ls datalake/
# Should show date folders: 20251007/, etc.
```

### 2. Verify MongoDB
```bash
mongosh

use gutenberg_search
db.books.countDocuments()        # Should show 500 (or your configured number)
db.words.countDocuments()        # Should show ~45,000 unique terms
```

### 3. Verify inverted index
```bash
python BasicQueryEngine.py
# Perform searches and verify results are returned
```

## 🛠️ Troubleshooting

### Error: `ImportError: No module named 'pymongo'`
```bash
pip install pymongo
```

### Error: MongoDB connection refused
1. Verify MongoDB is running
2. Check port (should be 27017)
3. Restart MongoDB service

### Error: `UnicodeDecodeError` in PostgreSQL
Use MongoDB instead of PostgreSQL (already configured by default)

### Books not downloading
- Check internet connection
- Some Gutenberg IDs may not exist (this is normal)
- Crawler automatically continues with next ID

## 📝 Technical Notes

### Data Organization
- **Datalake**: Books organized by download date/time `YYYYMMDD/HH/`
- **Control**: Plain text files for progress tracking
- **MongoDB**: Two main collections:
  - `books`: Book metadata
  - `words`: Inverted index (term → [book_ids])

### Processing Pipeline
1. **Download**: Fetches plain text (.txt) from Gutenberg
2. **Extraction**: Parses metadata from book header
3. **Tokenization**: Text cleaning and normalization
4. **Indexing**: Inverted index construction

### Scalability
- Pipeline is **resumable**: If interrupted, continues from last processed book
- **Modular** design: Easy to swap storage implementations
- **Phase separation**: Download and processing can run independently

## 👥 Authors

- Juan Diego ([juand4569](https://github.com/juand4569))
- [Other collaborators]

## 📄 License

This project is part of a Big Data academic assignment.

---

**Project Gutenberg Notice**: Downloaded books come from [Project Gutenberg](https://www.gutenberg.org/) and are in the public domain.