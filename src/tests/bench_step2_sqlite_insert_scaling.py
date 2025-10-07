
import sys, pathlib, random, string, time
from datetime import date
from pathlib import Path

# Importar desde src/
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

from src.metadata.SQLiteMetadataStorage import SQLiteMetadataStorage
from src.metadata.BookMetadata import BookMetadata

random.seed(7)
SIZES = [962, 3885, 14293]
OUTDIR = Path("bench_out"); OUTDIR.mkdir(parents=True, exist_ok=True)

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

def run_once(n: int) -> float:
    db_path = OUTDIR / f"step2_metadata_{n}.sqlite"
    if db_path.exists():
        db_path.unlink()  # BD limpia por tamaño
    st = SQLiteMetadataStorage(db_path=str(db_path))
    if not st.initialize():
        raise SystemExit(f"ERROR: initialize() False para {n}")
    items = make_dataset(n)
    t0 = time.perf_counter()
    for it in items:
        st.insert_book_metadata(it)
    elapsed = time.perf_counter() - t0
    st.close()
    return elapsed

def main():
    for n in SIZES:
        seconds = run_once(n)
        print(f"INSERT_SQLITE_SECONDS_{n}={seconds:.2f}")

if __name__ == "__main__":
    main()
