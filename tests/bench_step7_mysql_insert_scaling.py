# tests/bench_step7_mysql_insert_scaling.py
import os, random, string, time
from datetime import date
from statistics import mean

# --- credenciales por variables de entorno ---
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASS = os.getenv("MYSQL_PASS", "")
MYSQL_DB   = os.getenv("MYSQL_DB", "bigdata_bench")

SIZES = [962, 3885, 14293]
TRIALS = 3

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

def create_db_and_table(cnn):
    c = cnn.cursor()
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
    c.close()

def measure_once(n: int):
    import mysql.connector as mc
    # conectar al server y asegurar DB
    root = mc.connect(host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASS, autocommit=True)
    cur = root.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {MYSQL_DB}")
    cur.close(); root.close()

    cnn = mc.connect(host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASS, database=MYSQL_DB)
    create_db_and_table(cnn)

    rows = make_rows(n)
    c = cnn.cursor()
    t0 = time.perf_counter()
    c.executemany(
        "INSERT INTO books(book_id,title,author,language,release_date) VALUES(%s,%s,%s,%s,%s)",
        rows
    )
    cnn.commit()
    elapsed = time.perf_counter() - t0
    c.close(); cnn.close()
    return elapsed

def main():
    random.seed(7)
    try:
        import mysql.connector  # noqa: F401
    except Exception:
        print("ERROR: falta mysql-connector-python. Instala con: pip install mysql-connector-python")
        return

    for n in SIZES:
        times = []
        for _ in range(TRIALS):
            try:
                times.append(measure_once(n))
            except Exception as e:
                print(f"ERROR_MYSQL_{n}: {e}")
                times = []
                break
        if times:
            print(f"INSERT_MYSQL_SECONDS_{n}={mean(times):.2f}")
        else:
            print(f"INSERT_MYSQL_SECONDS_{n}=N/A")

if __name__ == "__main__":
    main()
