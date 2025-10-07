# tests/bench_step3_sqlite_query_perf_3885.py
import sys, pathlib, random, string, time
from datetime import date
from pathlib import Path

# importar desde src/
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

from src.metadata.SQLiteMetadataStorage import SQLiteMetadataStorage
from src.metadata.BookMetadata import BookMetadata

random.seed(7)
DB_PATH = Path("bench_out/step3_metadata.sqlite")
DB_PATH.parent.mkdir(parents=True, exist_ok=True)
if DB_PATH.exists():
    DB_PATH.unlink()  # BD limpia

LANGS = ["en","es","fr","de","it","pt"]

def rand_title():
    return " ".join(
        "".join(random.choices(string.ascii_letters, k=random.randint(3,8)))
        for _ in range(random.randint(2,6))
    ).title()

def make_dataset(n: int):
    items = []
    for i in range(1, n+1):
        items.append(
            BookMetadata(
                book_id=i,
                title=rand_title(),
                author=f"Author_{random.randint(0,199)}",
                language=random.choices(LANGS, weights=[55,18,8,7,6,6])[0],
                release_date=date(
                    1900 + random.randint(0,120),
                    random.randint(1,12),
                    random.randint(1,28)
                ).isoformat()
            )
        )
    return items

# 1) Inicializa e inserta 3885
st = SQLiteMetadataStorage(db_path=str(DB_PATH))
ok = st.initialize()
if not ok:
    raise SystemExit("ERROR: initialize() devolvió False")

items = make_dataset(3885)
for it in items:
    st.insert_book_metadata(it)

# Elegimos claves que EXISTEN para no medir vacíos
author_target = items[100].author
lang_target = "en"
word_target = items[200].title.split(" ")[0].lower()  # primera palabra del título 200

# 2) Medición de consultas
t0 = time.perf_counter()
res_author = st.get_books_by_author(author_target)
t_author = time.perf_counter() - t0

t0 = time.perf_counter()
res_lang = st.get_books_by_language(lang_target)
t_lang = time.perf_counter() - t0


t0 = time.perf_counter()
all_books = st.get_all_books()
res_title = [b for b in all_books if word_target in (b.title or "").lower()]
t_title = time.perf_counter() - t0

st.close()

total = t_author + t_lang + t_title

# 3) Salida simple
print(f"QUERY_SQLITE_3885_AUTHOR_SECONDS={t_author:.2f}  RESULTS={len(res_author)}  KEY={author_target}")
print(f"QUERY_SQLITE_3885_LANGUAGE_SECONDS={t_lang:.2f} RESULTS={len(res_lang)}   KEY={lang_target}")
print(f"QUERY_SQLITE_3885_TITLECONTAINS_SECONDS={t_title:.2f} RESULTS={len(res_title)} KEYWORD={word_target}")
print(f"QUERY_SQLITE_3885_TOTAL_SECONDS={total:.2f}")
