import pytest
from pathlib import Path


@pytest.fixture(scope="session")
def sample_books_path():
    """
    Ruta base donde guardar o descargar libros (por ejemplo en datalake).
    Ajustar proyecto si usa otra ruta.
    """
    base = Path(__file__).resolve().parent.parent / "src" / "data"
    base.mkdir(parents=True, exist_ok=True)
    return base

@pytest.fixture(scope="session")
def short_book_text():
    """Texto breve: ideal para probar tokenización."""
    return (
        "Capítulo I\n"
        "Era un día soleado cuando el joven protagonista encontró el misterioso libro."
        " El viento movía las páginas con suavidad."
    )

@pytest.fixture(scope="session")
def medium_book_text():
    """Texto mediano con frases más variadas, simula parte de un libro real."""
    paragraph = (
        "El protagonista continuó su viaje por la ciudad antigua, observando cómo la luz "
        "se filtraba entre los muros. A medida que avanzaba, recordaba los secretos del "
        "manuscrito que había encontrado. "
    )
    return paragraph * 50

@pytest.fixture(scope="session")
def book_documents():
    """
    Crea un conjunto de documentos (id → texto) para pruebas del indexador.
    """
    docs = {}
    for i in range(100):
        docs[f"book_{i}"] = f"Título {i}. Contenido del libro {i} con palabras repetidas y temas similares."
    return docs
