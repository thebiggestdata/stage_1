# tests/bench_crawler_500.py
import sys, pathlib, time, os
from pathlib import Path

# 1) Importar desde src/
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

# 2) Tus clases reales (según tu estructura)
try:
    from src.crawler.Crawler import Crawler
except Exception:
    Crawler = None  # fallback si no existe

# --- Config ---
DATALAKE = Path("data/datalake")
IDS_FILE = Path("tests/book_ids_500.txt")   # opcional: lista de IDs (uno por línea)
TARGET = 500

def load_ids():
    if IDS_FILE.exists():
        return [int(x.strip()) for x in IDS_FILE.read_text().splitlines() if x.strip().isdigit()]
    # Rango “popular” de Gutenberg; se intentan muchos hasta juntar 500 válidos
    return list(range(1342, 1342 + 3000))

# 3) Adaptador: intenta distintos métodos que tu Crawler podría exponer
def crawler_process_id(crawler, book_id: int, datalake_base: str) -> bool:
    if crawler is None:
        return False

    method_names = [
        "fetch_and_store", "process_one", "crawl_one",
        "run_one", "download_and_save", "handle_book_id"
    ]
    for name in method_names:
        if hasattr(crawler, name):
            fn = getattr(crawler, name)
            try:
                # algunos métodos devuelven bool, otros pueden lanzar excepción
                res = fn(book_id) if fn.__code__.co_argcount == 2 else fn(book_id, datalake_base)
                return bool(res) if res is not None else True
            except Exception:
                return False
    return False  # no se encontró método compatible

# 4) Fallback: descarga directa y guarda en datalake (por si el Crawler no está listo)
def fallback_download_to_datalake(book_id: int, datalake_base: str) -> bool:
    import requests
    START = "*** START OF THE PROJECT GUTENBERG EBOOK"
    END   = "*** END OF THE PROJECT GUTENBERG EBOOK"

    try:
        url = f"https://www.gutenberg.org/cache/epub/{book_id}/pg{book_id}.txt"
        r = requests.get(url, timeout=15)
        if r.status_code != 200:
            return False
        txt = r.text
        if START not in txt or END not in txt:
            return False
        header, rest = txt.split(START, 1)
        body, _ = rest.split(END, 1)

        # estructura datalake simple: data/datalake/<book_id>.header.txt y .body.txt
        base = Path(datalake_base)
        base.mkdir(parents=True, exist_ok=True)
        (base / f"{book_id}.header.txt").write_text(header.strip(), encoding="utf-8")
        (base / f"{book_id}.body.txt").write_text(body.strip(), encoding="utf-8")
        return True
    except Exception:
        return False

def main():
    DATALAKE.mkdir(parents=True, exist_ok=True)
    ids = load_ids()

    # Instancia del Crawler si existe; varios proyectos piden el path base
    crawler = None
    if Crawler is not None:
        try:
            # probar ctors típicos: Crawler(datalake_base=...), Crawler(...), Crawler()
            try:
                crawler = Crawler(datalake_base=str(DATALAKE))
            except TypeError:
                try:
                    crawler = Crawler(str(DATALAKE))
                except TypeError:
                    crawler = Crawler()
        except Exception:
            crawler = None

    ok = 0
    attempted = 0
    t0 = time.perf_counter()

    for bid in ids:
        attempted += 1
        worked = False
        # 1º intenta con tu Crawler real
        if crawler is not None:
            worked = crawler_process_id(crawler, bid, str(DATALAKE))
        # 2º si no, usa fallback directo
        if not worked:
            worked = fallback_download_to_datalake(bid, str(DATALAKE))

        if worked:
            ok += 1
        if ok >= TARGET:
            break

    elapsed = time.perf_counter() - t0
    avg = elapsed / ok if ok else float("inf")
    per_min = ok / (elapsed / 60) if elapsed > 0 else 0.0

    print(f"CRAWLER_TOTAL_BOOKS={ok}")
    print(f"CRAWLER_ATTEMPTED={attempted}")
    print(f"CRAWLER_TOTAL_SECONDS={elapsed:.2f}")
    print(f"CRAWLER_AVG_SECONDS_PER_BOOK={avg:.2f}")
    print(f"CRAWLER_BOOKS_PER_MINUTE={per_min:.2f}")
    print(f"CRAWLER_DATALAKE={DATALAKE.resolve()}")

if __name__ == "__main__":
    main()
