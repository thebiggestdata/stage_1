# tests/bench_step6_mysql_insert_3885.py
import os, random, string, time
from datetime import date

# --- credenciales por variables de entorno (modifícalas si quieres) ---
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASS = os.getenv("MYSQL_PASS", "")
MYSQL_DB   = os.getenv("MYSQL_DB", "bigdata_bench")

def rand_title():
    return " ".join(
        "".join(random.choices(string.ascii_letters, k=random.randint(3,8)))
        for _ in range(random.randint(2,6))
    ).title()

def make_rows(n: int):
    rows = []
    langs = ["en","es","fr","de","it","pt"]
    for i in range(1, n+1):
        rows.append((
            i,
            rand_title(),
            f"Author_{random.randint(0,199)}",
            random.choices(langs, weights=[55,18,8,7,6,6])[0],
            date(1900 + random.randint(0,120), random.randint(1,12), random.randint(1,28)).isoformat()
        ))
    return rows

def main():
    random.seed(7)
    try:
        import mysql.connector as mc
    except Exception:
        print("ERROR: falta el paquete mysql-connector-python. Instala con: pip install mysql-connector-python")
        return

    # 1) Conexión al servidor y creación de DB si no existe
    try:
        root = mc.connect(host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASS, autocommit=True)
    except Exception as e:
        print(f"ERROR: no puedo conectar a MySQL ({MYSQL_USER}@{MYSQL_HOST}): {e}")
        return
    cur = root.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {MYSQL_DB}")
    cur.close(); root.close()

    # 2) Conexión a la DB
    try:
        cnn = mc.connect(host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASS, database=MYSQL_DB)
    except Exception as e:
        print(f"ERROR: no puedo abrir la BD '{MYSQL_DB}': {e}")
        return
    c = cnn.cursor()

    # 3) Tabla limpia + índices
    c.execute("DROP TABLE IF EXISTS books")
    c.execute("""
        CREATE TABLE books(
            book_id INT PRIMARY KEY,
            title TEXT,
            author VARCHAR(255),
            language VARCHAR(16),
            release_date DATE,
            INDEX idx_author(author),
            INDEX idx_language(language),
            INDEX idx_title_prefix (title(100))
        ) ENGINE=InnoDB
    """)
    cnn.commit()

    # 4) Datos y medición
    rows = make_rows(3885)

    t0 = time.perf_counter()
    c.executemany(
        "INSERT INTO books(book_id,title,author,language,release_date) VALUES(%s,%s,%s,%s,%s)",
        rows
    )
    cnn.commit()
    elapsed = time.perf_counter() - t0

    # 5) Cierre y salida
    c.close(); cnn.close()
    print(f"INSERT_3885_MYSQL_SECONDS={elapsed:.2f}")

if __name__ == "__main__":
    main()
