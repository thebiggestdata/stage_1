# tests/bench_indexer_500.py
import sys, pathlib, time, inspect
from pathlib import Path

# -------- Importar desde la raíz del repo --------
PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

# -------- Imports de tu proyecto --------
from src.indexer.TextTokenizer import TextTokenizer
try:
    from src.indexer.SQLiteInvertedIndex import SQLiteInvertedIndex
except Exception:
    SQLiteInvertedIndex = None

# Soporta ambos nombres (Hierarchicall vs Hierarchical)
HierIndexClass = None
try:
    from src.indexer.HierarchicalInvertedIndex import HierarchicalInvertedIndex as HierIndexClass
except Exception:
    try:
        from src.indexer.HierarchicalInvertedIndex import HierarchicalInvertedIndex as HierIndexClass
    except Exception:
        HierIndexClass = None

# -------- Rutas --------
DATALAKE = PROJECT_ROOT / "data" / "datalake"
OUTDIR = PROJECT_ROOT / "bench_out"
OUTDIR.mkdir(parents=True, exist_ok=True)

# -------- Utilidades --------
def pick_bodies(max_n=500):
    """Devuelve hasta max_n archivos *.body.txt y el total encontrado."""
    files = sorted(DATALAKE.rglob("*.body.txt"))
    return files[:max_n], len(files)

def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="ignore")

_used_method_printed = False

def _maybe_init(index_obj):
    for m in ("initialize", "init", "open", "start"):
        if hasattr(index_obj, m):
            try:
                getattr(index_obj, m)()
            except TypeError:
                try:
                    getattr(index_obj, m)(str(OUTDIR))
                except Exception:
                    pass
            break

def add_to_index(index_obj, doc_id: str, tokens, text: str):
    """
    Descubre y llama al método correcto del índice.
    Estrategia:
      1) Buscar métodos públicos que contengan 'add' o 'index'
      2) Inspeccionar firma: si tiene un parámetro llamado 'tokens', 'terms' -> pasar tokens;
         si 'text', 'content' -> pasar text; si 2 args posicionales -> probar primero tokens y luego text.
    """
    global _used_method_printed

    # 1) candidatos explícitos primero
    explicit_candidates = [
        "add_document", "index_document", "add", "index",
        "add_tokens", "add_terms", "index_tokens", "index_terms",
        "insert", "upsert", "put"
    ]
    methods = []
    for name in explicit_candidates:
        if hasattr(index_obj, name) and callable(getattr(index_obj, name)):
            methods.append(name)

    # 2) si no encontramos, explorar cualquier público con 'add' o 'index'
    if not methods:
        for name in dir(index_obj):
            if name.startswith("_"):
                continue
            if ("add" in name.lower()) or ("index" in name.lower()):
                attr = getattr(index_obj, name)
                if callable(attr):
                    methods.append(name)

    last_err = None
    for name in methods:
        fn = getattr(index_obj, name)
        try:
            sig = inspect.signature(fn)
        except (TypeError, ValueError):
            sig = None

        # construir argumentos
        args_ok = False
        call_tokens_first = True

        if sig:
            params = [p for p in sig.parameters.values() if p.name != "self"]
            names = [p.name.lower() for p in params]
            # - detecta por nombre
            if any(n in ("tokens", "terms", "tok", "words") for n in names):
                arg = tokens
                args_ok = True
            elif any(n in ("text", "content", "doc", "document") for n in names):
                arg = text
                args_ok = True
                call_tokens_first = False
            # - si hay exactamente 2 posicionales (doc_id, algo)
            elif len(params) >= 2:
                arg = tokens  # probamos tokens primero
                args_ok = True
            else:
                # firma rara, probamos aún así
                arg = tokens
                args_ok = True
        else:
            arg = tokens
            args_ok = True

        if not args_ok:
            continue

        # intentos: (doc_id, tokens) y si falla, (doc_id, text)
        try:
            fn(doc_id, arg)
            if not _used_method_printed:
                print(f"INFO: usando método de indexación '{name}' ({'tokens' if call_tokens_first else 'text'})")
                _used_method_printed = True
            return
        except TypeError as e:
            last_err = e
            # probar con el otro tipo
            try:
                fn(doc_id, text if arg is tokens else tokens)
                if not _used_method_printed:
                    print(f"INFO: usando método de indexación '{name}' (fallback)")
                    _used_method_printed = True
                return
            except Exception as e2:
                last_err = e2
                continue
        except Exception as e:
            last_err = e
            continue

    # si llegamos aquí, no se pudo indexar
    raise AttributeError(f"No encontré un método válido para indexar en {type(index_obj).__name__}. Último error: {last_err}")

def bench_one_index(index_name: str, index_obj, doc_paths):
    """Indexa doc_paths y muestra métricas."""
    _maybe_init(index_obj)
    tok = TextTokenizer()
    unique_terms = set()
    total_tokens = 0

    t0 = time.perf_counter()
    for p in doc_paths:
        text = read_text(p)
        tokens = tok.tokenize(text)  # <- API real del tokenizador
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
    docs, total_found = pick_bodies(500)
    if total_found < 500:
        print(f"AVISO: encontré {total_found} cuerpos en {DATALAKE}. "
              f"Indexaré {len(docs)} disponibles. Ejecuta el crawler para llegar a 500.")
        if len(docs) == 0:
            return

    # ---- SQLite ----
    if SQLiteInvertedIndex is not None:
        idx_path = OUTDIR / "index_sqlite_bench.db"
        if idx_path.exists():
            idx_path.unlink()
        idx = SQLiteInvertedIndex(str(idx_path))
        bench_one_index("SQLite", idx, docs)
        if hasattr(idx, "close"):
            try: idx.close()
            except Exception: pass

    # ---- Jerárquico (si existe) ----
    if HierIndexClass is not None:
        idx_dir = OUTDIR / "index_hier_bench"
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
