import os, time, random, string
from datetime import date
from pymongo import MongoClient, ASCENDING

MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017")
MONGO_DB  = os.getenv("MONGO_DB",  "bigdata_bench")

def rand_title():
    return " ".join(
        "".join(random.choices(string.ascii_letters, k=random.randint(3,8)))
        for _ in range(random.randint(2,6))
    ).title()

def make_docs(n=3885):
    langs = ["en","es","fr","de","it","pt"]
    docs = []
    for i in range(1, n+1):
        docs.append({
            "_id": i,            # clave primaria natural
            "book_id": i,
            "title": rand_title(),
            "author": f"Author_{random.randint(0,199)}",
            "language": random.choices(langs, weights=[55,18,8,7,6,6])[0],
            "release_date": date(1900 + random.randint(0,120),
                                 random.randint(1,12),
                                 random.randint(1,28)).isoformat()
        })
    return docs

def main():
    random.seed(7)
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    col = db["books_meta"]

    # Reset colección + índices (equivalente a SQLite con índices en author/language/tite)
    col.drop()
    col.create_index([("author",   ASCENDING)], name="idx_author")
    col.create_index([("language", ASCENDING)], name="idx_language")
    col.create_index([("title",    ASCENDING)], name="idx_title")

    docs = make_docs(3885)

    t0 = time.perf_counter()
    col.insert_many(docs, ordered=False)
    elapsed = time.perf_counter() - t0

    print(f"INSERT_3885_MONGO_SECONDS={elapsed:.2f}")
    print(f"INSERT_3885_MONGO_AVG_PER_BOOK={elapsed/len(docs):.6f}")

if __name__ == "__main__":
    main()
