# tests/bench_step5_flatfiles_insert_scaling.py
import random, string, time, json, csv
from datetime import date
from pathlib import Path
from statistics import mean

random.seed(7)

SIZES = [962, 3885, 14293]
TRIALS = 3
OUTDIR = Path("bench_out"); OUTDIR.mkdir(parents=True, exist_ok=True)

def rand_title():
    return " ".join(
        "".join(random.choices(string.ascii_letters, k=random.randint(3,8)))
        for _ in range(random.randint(2,6))
    ).title()

def make_rows(n: int):
    rows = []
    for i in range(1, n+1):
        rows.append({
            "book_id": i,
            "title": rand_title(),
            "author": f"Author_{random.randint(0,199)}",
            "language": random.choices(["en","es","fr","de","it","pt"],
                                       weights=[55,18,8,7,6,6])[0],
            "release_date": date(
                1900 + random.randint(0,120),
                random.randint(1,12),
                random.randint(1,28)
            ).isoformat()
        })
    return rows

def write_csv(rows, path: Path):
    t0 = time.perf_counter()
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["book_id","title","author","language","release_date"])
        w.writeheader()
        w.writerows(rows)
    return time.perf_counter() - t0

def write_jsonl(rows, path: Path):
    t0 = time.perf_counter()
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    return time.perf_counter() - t0

def run_avg(n: int):
    rows = make_rows(n)
    csv_times, json_times = [], []

    for _ in range(TRIALS):
        csv_path = OUTDIR / f"step5_books_{n}.csv"
        jsonl_path = OUTDIR / f"step5_books_{n}.jsonl"
        if csv_path.exists(): csv_path.unlink()
        if jsonl_path.exists(): jsonl_path.unlink()

        csv_times.append(write_csv(rows, csv_path))
        json_times.append(write_jsonl(rows, jsonl_path))

    return mean(csv_times), mean(json_times)

def main():
    for n in SIZES:
        t_csv, t_json = run_avg(n)

        print(f"INSERT_CSV_SECONDS_{n}={t_csv:.2f}")
        print(f"INSERT_JSON_SECONDS_{n}={t_json:.2f}")

if __name__ == "__main__":
    main()
