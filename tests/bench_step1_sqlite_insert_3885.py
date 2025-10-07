# tests/bench_step1_sqlite_insert_3885.py
import sys, pathlib, random, string, time
from datetime import date
from pathlib import Path

# --- para poder importar desde src/ ---
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

from src.metadata.SQLiteMetadataStorage import SQLiteMetadataStorage
from src.metadata.BookMetadata import BookMetadata

# --- configuración ---
random.seed(7)
DB_PATH = Path("bench_out/step1_metadata.sqlite")
DB_PATH.parent.mkdir(parents=True, exist_ok=True)
if DB_PATH.exists():
    DB_PATH.unlink()  # empezamos con BD limpia

# --- helpers de datos sintéticos ---
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
                language=random.choices(["en","es","fr","de","it","pt"],
                                        weights=[55,18,8,7,6,6])[0],
                release_date=date(
                    1900 + random.randint(0,120),
                    random.randint(1,12),
                    random.randint(1,28)
                ).isoformat()
            )
        )
    return items

# --- inicializa storage ---
st = SQLiteMetadataStorage(db_path=str(DB_PATH))
ok = st.initialize()
if not ok:
    raise SystemExit("ERROR: initialize() devolvió False")

# --- genera y mide inserción ---
items = make_dataset(3885)

t0 = time.perf_counter()
for it in items:
    # tu método devuelve bool; si falla alguno, no paramos la prueba
    st.insert_book_metadata(it)
elapsed = time.perf_counter() - t0

st.close()

# --- salida simple (sin tablas) ---
print(f"INSERT_3885_SQLITE_SECONDS={elapsed:.2f}")
