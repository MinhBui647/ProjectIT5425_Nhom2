import argparse
import json
import resource
import sqlite3
import statistics
import subprocess
import sys
import time
from pathlib import Path

import numpy as np

DATA_DIR = Path("data/benchmark")
OUT_DIR = Path("reports/benchmark")
ENGINES = ["pandas", "sqlite", "duckdb", "chdb"]
QUERIES = ["agg_by_batch", "p95_by_batch", "minute_rollup"]
COLORS = {"chdb": "#184C78", "duckdb": "#6B7280", "pandas": "#9CA3AF", "sqlite": "#D1D5DB"}

SQL = {
    "agg_by_batch": "SELECT batch_no, avg(uht_temp), max(uht_temp), count(*) FROM t GROUP BY batch_no",
    "p95_by_batch": {
        "duckdb": "SELECT batch_no, quantile_cont(uht_temp, 0.95) FROM t GROUP BY batch_no",
        "chdb": "SELECT batch_no, quantileTDigest(0.95)(uht_temp) FROM t GROUP BY batch_no",
    },
    "minute_rollup": {
        "sqlite": "SELECT line_id, ts_s / 60 AS m, avg(uht_temp), avg(power_kw) FROM t GROUP BY line_id, m",
        "duckdb": "SELECT line_id, ts_s // 60 AS m, avg(uht_temp), avg(power_kw) FROM t GROUP BY line_id, m",
        "chdb": "SELECT line_id, intDiv(ts_s, 60) AS m, avg(uht_temp), avg(power_kw) FROM t GROUP BY line_id, m",
    },
}


def sql_for(query: str, engine: str) -> str | None:
    q = SQL[query]
    return q if isinstance(q, str) else q.get(engine)


def dataset_path(n: int) -> Path:
    return DATA_DIR / f"sensor_{n}.parquet"


def make_dataset(n: int) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    path = dataset_path(n)
    if path.exists():
        return
    rng = np.random.default_rng(5425)
    table = pa.table({
        "ts_s": np.int64(1_767_225_600) + np.arange(n, dtype=np.int64),
        "line_id": rng.choice(np.array(["LINE_UHT_1", "LINE_UHT_2"]), n),
        "batch_no": (np.arange(n) // 7200).astype(np.int32),
        "uht_temp": rng.normal(140, 1.5, n).astype(np.float32),
        "flow_rate": rng.normal(12000, 300, n).astype(np.float32),
        "power_kw": rng.normal(250, 10, n).astype(np.float32),
    })
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)


# ---- loaders: load data into the engine, return run(query) -> n_rows | None

def load_pandas(path):
    import pandas as pd

    df = pd.read_parquet(path)

    def run(query):
        if query == "agg_by_batch":
            return len(df.groupby("batch_no")["uht_temp"].agg(["mean", "max", "count"]))
        if query == "p95_by_batch":
            return len(df.groupby("batch_no")["uht_temp"].quantile(0.95))
        return len(df.groupby([df["line_id"], df["ts_s"] // 60])[["uht_temp", "power_kw"]].mean())

    return run


def load_sqlite(path):
    import pyarrow.parquet as pq

    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE t (ts_s INTEGER, line_id TEXT, batch_no INTEGER, "
                "uht_temp REAL, flow_rate REAL, power_kw REAL)")
    for b in pq.ParquetFile(path).iter_batches(batch_size=100_000):
        rows = zip(*(b.column(i).to_pylist() for i in range(b.num_columns)))
        con.executemany("INSERT INTO t VALUES (?, ?, ?, ?, ?, ?)", rows)

    def run(query):
        sql = sql_for(query, "sqlite")
        return None if sql is None else len(con.execute(sql).fetchall())

    return run


def load_duckdb(path):
    import duckdb

    con = duckdb.connect()
    con.execute(f"CREATE TABLE t AS SELECT * FROM read_parquet('{path.resolve().as_posix()}')")

    def run(query):
        return len(con.execute(sql_for(query, "duckdb")).fetchall())

    return run


def load_chdb(path):
    from chdb import session as chs

    sess = chs.Session()
    sess.query("CREATE TABLE t (ts_s Int64, line_id LowCardinality(String), batch_no Int32, "
               "uht_temp Float32, flow_rate Float32, power_kw Float32) "
               "ENGINE = MergeTree ORDER BY (line_id, ts_s)")
    sess.query(f"INSERT INTO t SELECT * FROM file('{path.resolve().as_posix()}', Parquet)")

    def run(query):
        return len(sess.query(sql_for(query, "chdb"), "DataFrame"))

    return run


LOADERS = {"pandas": load_pandas, "sqlite": load_sqlite, "duckdb": load_duckdb, "chdb": load_chdb}


def run_worker(engine: str, n: int, query: str, repeats: int) -> None:
    t0 = time.perf_counter()
    run = LOADERS[engine](dataset_path(n))
    load_s = time.perf_counter() - t0
    # First (cold) run is timed separately: it includes one-off engine warm-up
    # (chDB ~0.1-0.2 s) that would otherwise dominate small queries.
    t = time.perf_counter()
    n_out = run(query)
    cold_s = time.perf_counter() - t
    times = []
    for _ in range(repeats):
        t = time.perf_counter()
        n_out = run(query)
        times.append(time.perf_counter() - t)
    peak_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    print(json.dumps({
        "engine": engine, "n_rows": n, "query_name": query, "load_s": round(load_s, 4),
        "cold_s": None if n_out is None else round(cold_s, 5),
        "latency_s": None if n_out is None else round(statistics.median(times), 5),
        "result_rows": n_out, "peak_rss_mb": round(peak_mb, 1),
    }))


def plot(df) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    for q in QUERIES:
        fig, ax = plt.subplots(figsize=(7, 4.5))
        for engine in ENGINES:
            d = df[(df.query_name == q) & (df.engine == engine) & df.latency_s.notna()].sort_values("n_rows")
            if d.empty:
                continue
            ax.plot(d.n_rows, d.latency_s, marker="o", color=COLORS[engine], lw=2.5 if engine == "chdb" else 1.5)
            ax.text(d.n_rows.iloc[-1] * 1.08, d.latency_s.iloc[-1], engine, va="center", color=COLORS[engine])
        ax.set(xscale="log", yscale="log", xlabel="Số dòng cảm biến", ylabel="Thời gian truy vấn (s, median)",
               title=f"Latency vs Data Size: {q}")
        ax.spines[["top", "right"]].set_visible(False)
        ax.grid(axis="y", alpha=0.3)
        fig.tight_layout()
        fig.savefig(OUT_DIR / f"latency_{q}.png", dpi=150)
        plt.close(fig)

    biggest = df[df.n_rows == df.n_rows.max()].groupby("engine")["peak_rss_mb"].max().reindex(ENGINES).dropna()
    fig, ax = plt.subplots(figsize=(6, 4))
    bars = ax.bar(biggest.index, biggest.values, color=[COLORS[e] for e in biggest.index])
    ax.bar_label(bars, fmt="%.0f MB")
    ax.set(ylabel="RAM đỉnh (MB)", title=f"Peak Memory @ {df.n_rows.max():,} dòng")
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    fig.savefig(OUT_DIR / "peak_memory.png", dpi=150)
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sizes", nargs="+", type=int, default=[100_000, 1_000_000, 5_000_000])
    parser.add_argument("--engines", nargs="+", default=ENGINES)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--worker", nargs=3, metavar=("ENGINE", "N", "QUERY"))
    args = parser.parse_args()

    if args.worker:
        engine, n, query = args.worker
        run_worker(engine, int(n), query, args.repeats)
        return

    import pandas as pd

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    results = []
    for n in args.sizes:
        make_dataset(n)
        for engine in args.engines:
            for q in QUERIES:
                cmd = [sys.executable, "-m", "olap_engineering.benchmark",
                       "--worker", engine, str(n), q, "--repeats", str(args.repeats)]
                proc = subprocess.run(cmd, capture_output=True, text=True)
                if proc.returncode != 0:
                    print(f"[fail] {engine} n={n} {q}\n{proc.stderr[-2000:]}")
                    continue
                res = json.loads(proc.stdout.strip().splitlines()[-1])
                results.append(res)
                print(res)
    df = pd.DataFrame(results)
    df.to_csv(OUT_DIR / "benchmark_results.csv", index=False)
    plot(df)
    print(f"Saved results and charts to {OUT_DIR}/")


if __name__ == "__main__":
    main()