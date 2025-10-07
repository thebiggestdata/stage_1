# tests/bench_indexer_500.py
import sys, pathlib, time
from pathlib import Path

# Permitir importar desde src/
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

# Tokenizador real de tu proyecto
from src.indexer.TextTokenizer import TextTokenizer

# Índices (probamos nombres de archivo/clase con y sin la 'l' extra)
try:
    from src.indexer.SQLiteInvertedIndex import SQLiteInvertedIndex
except Exception:
    SQLiteInvertedIndex = None

HierIndexClass = None
try:
    from src.indexer.HierarchicalInvertedIndex import HierarchicalInvertedIndex as HierIndexClass
except Exception:
    try:
        from src.indexer.HierarchicalInvertedIndex import HierarchicalInvertedIndex as HierIndexClass
    except Exception:
        HierIndexClass = None

DATALAKE = Path("data/datalake")
OUTDIR = Path("bench_out"); OUTDIR.mkdir(parents=True, exist_ok=True)

def pick_500_bodies():
    """Busca 500 archivos *.body.txt en el datalake (recursivo)."""
    files = sorted(DATALAKE.rglob("*.body.txt"))
    return files[:500]

def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="ignore")

def add_to_index(index_obj, doc_id: str, tokens, text: str):
    """
    Adaptador para diferentes APIs de índice.
    1) intenta add_document(doc_id, tokens)
    2) add_document(doc_id, text)
    3) add(doc_id, tokens) / index_document / index
    """
    # candidatos (nombre, usa_tokens)
    candidates = [
        ("add_document", True),
        ("add_document", False),
        ("add", True),
        ("add", False),
        ("index_document", True),
        ("index_document", False),
        ("index", True),
        ("index", False),
    ]
    for name, use_tokens in candidates:
        if hasattr(index_obj, name):
            fn = getattr(index_obj, name)
            try:
                return fn(doc_id, tokens if use_tokens else text)
            except TypeError:
                # cambia de firma; probamos la otra variante
                continue
    # último intento: call genérico con kwargs comunes
    try:
        return index_obj.add_document(doc_id=doc_id, tokens=tokens)
    except Exception:
        return index_obj.add_document(doc_id=doc_id, text=text)

def bench_one_index(index_name: str, index_obj, doc_paths):
    """Indexa 500 libros y devuelve métricas."""
    tok = TextTokenizer()
    unique_terms = set()
    total_tokens = 0

    t0 = time.perf_counter()
    for p in doc_paths:
        text = read_text(p)
        tokens = tok.tokenize(text)  # <- tu API real
        total_tokens += len(tokens)
        unique_terms.update(tokens)
        doc_id = p.stem.replace(".body", "")
        add_to_index(index_obj, doc_id, tokens, text)
    elapsed = time.perf_counter() - t0
    avg = elapsed / len(doc_paths) if doc_paths else float("inf")

    print(f"INDEXER[{index_name}]_DOCS={len(doc_paths)}")
    print(f"INDEXER[{index_name}]_TOTAL_SECONDS={elapsed:.2f}")
    print(f"INDEXER[{index_name}]_AVG_SECONDS_PER_DOC={avg:.2f}")
    print(f"INDEXER[{index_name}]_UNIQUE_TERMS={len(unique_terms)}")
    print(f"INDEXER[{index_name}]_TOTAL_TOKENS={total_tokens}")

def main():
    docs = pick_500_bodies()
    if len(docs) < 500:
        print(f"ERROR: solo encontré {len(docs)} cuerpos en {DATALAKE.resolve()}. Ejecuta el crawler primero.")
        return

    # ---- SQLiteInvertedIndex ----
    if SQLiteInvertedIndex is not None:
        idx_path = OUTDIR / "index_sqlite_bench.db"
        if idx_path.exists(): idx_path.unlink()
        idx = SQLiteInvertedIndex(str(idx_path))
        bench_one_index("SQLite", idx, docs)
        if hasattr(idx, "close"):
            idx.close()

    # ---- Hierarchical / Hierarchicall ----
    if HierIndexClass is not None:
        idx_dir = OUTDIR / "index_hier_bench"
        # limpiar carpeta si existe
        if idx_dir.exists():
            for x in sorted(idx_dir.rglob("*"), reverse=True):
                try: x.unlink()
                except: pass
            try: idx_dir.rmdir()
            except: pass
        hidx = HierIndexClass(str(idx_dir))
        bench_one_index("Hierarchical", hidx, docs)

if __name__ == "__main__":
    main()
