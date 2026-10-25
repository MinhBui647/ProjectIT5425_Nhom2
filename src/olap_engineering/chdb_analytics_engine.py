import argparse
import os
from pathlib import Path

from chdb import session as chs

from olap_engineering.gold_store import chdb_s3_source
from olap_engineering.star_schema import GOLD_TABLES, column_names, create_table_sql

DB = "dairy"
DEFAULT_GOLD_DIR = "data/03_gold_marts"
# Keep the DB on the Linux side (Docker volume): ClickHouse renames part
# directories, which fails with "Permission denied" on a Windows bind mount.
DEFAULT_DB_PATH = os.getenv("CHDB_PATH", "/chdb/factory_analytics.chdb")

AGG_MINUTE_DDL = [
    f"DROP VIEW IF EXISTS {DB}.mv_sensor_minute",
    f"DROP TABLE IF EXISTS {DB}.agg_sensor_minute",
    f"""CREATE TABLE {DB}.agg_sensor_minute (
        line_key UInt16,
        minute DateTime('UTC'),
        n_readings AggregateFunction(count),
        uht_temp_avg AggregateFunction(avg, Float32),
        uht_temp_p95 AggregateFunction(quantileTDigest(0.95), Float32),
        uht_temp_max SimpleAggregateFunction(max, Float32)
    ) ENGINE = AggregatingMergeTree ORDER BY (line_key, minute)""",
    f"""CREATE MATERIALIZED VIEW {DB}.mv_sensor_minute TO {DB}.agg_sensor_minute AS
        SELECT line_key, toStartOfMinute(timestamp) AS minute,
               countState() AS n_readings,
               avgState(uht_temp) AS uht_temp_avg,
               quantileTDigestState(0.95)(uht_temp) AS uht_temp_p95,
               max(uht_temp) AS uht_temp_max
        FROM {DB}.fact_sensor_telemetry
        GROUP BY line_key, minute""",
]

MINUTE_SUBQUERY = f"""
    SELECT line_key, minute, avgMerge(uht_temp_avg) AS avg_uht
    FROM {DB}.agg_sensor_minute GROUP BY line_key, minute"""

ANALYTICS_QUERIES = {
    "hourly_rollup_from_minute_states": f"""
        SELECT line_key, toStartOfHour(minute) AS hour,
               countMerge(n_readings) AS n,
               round(avgMerge(uht_temp_avg), 3) AS uht_avg,
               round(quantileTDigestMerge(0.95)(uht_temp_p95), 3) AS uht_p95,
               max(uht_temp_max) AS uht_max
        FROM {DB}.agg_sensor_minute
        GROUP BY line_key, hour ORDER BY line_key, hour LIMIT 24""",
    "p95_uht_by_batch": f"""
        SELECT b.batch_id,
               quantileTDigest(0.95)(s.uht_temp) AS uht_p95,
               max(s.uht_temp) AS uht_max, count() AS n
        FROM {DB}.fact_sensor_telemetry AS s
        JOIN {DB}.dim_batch AS b ON s.batch_key = b.batch_key
        GROUP BY b.batch_id ORDER BY uht_p95 DESC LIMIT 20""",
    "corr_uht_temp_yield": f"""
        SELECT corrStable(toFloat64(uht_temp_avg), toFloat64(yield_recovery_pct)) AS corr_uht_yield,
               corrStable(toFloat64(uht_temp_std), toFloat64(yield_recovery_pct)) AS corr_uht_std_yield,
               count() AS n_batches
        FROM {DB}.fact_batch_production WHERE n_readings > 0""",
    "uht_profile_groupArray": f"""
        SELECT line_key, toDate(minute) AS day,
               arrayMap(x -> x.2, arraySort(groupArray((minute, round(avg_uht, 2))))) AS uht_by_minute
        FROM ({MINUTE_SUBQUERY})
        GROUP BY line_key, day ORDER BY line_key, day LIMIT 5""",
    "uht_moving_avg_window": f"""
        SELECT line_key, minute, round(avg_uht, 3) AS avg_uht,
               round(avg(avg_uht) OVER w5, 3) AS uht_ma_5min,
               round(avg_uht - lagInFrame(avg_uht, 1, avg_uht) OVER w2, 3) AS uht_delta
        FROM ({MINUTE_SUBQUERY})
        WINDOW w5 AS (PARTITION BY line_key ORDER BY minute ROWS BETWEEN 4 PRECEDING AND CURRENT ROW),
               w2 AS (PARTITION BY line_key ORDER BY minute ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
        ORDER BY line_key, minute LIMIT 30""",
    "defect_rate_by_sku_shift": f"""
        SELECT p.sku_id, t.shift_code, count() AS n_batches,
               round(avg(f.batch_defect_flag), 3) AS defect_rate,
               round(avg(f.yield_recovery_pct), 2) AS avg_yield
        FROM {DB}.fact_batch_production AS f
        JOIN {DB}.dim_product AS p ON f.product_key = p.product_key
        JOIN {DB}.dim_time AS t ON f.start_time_key = t.time_key
        GROUP BY p.sku_id, t.shift_code ORDER BY p.sku_id, t.shift_code""",
    "heat_stress_vs_yield": f"""
        SELECT w.is_heat_stress, count() AS n_batches,
               round(avg(f.yield_recovery_pct), 2) AS avg_yield,
               round(avg(f.batch_defect_flag), 3) AS defect_rate
        FROM {DB}.fact_batch_production AS f
        JOIN {DB}.fact_external_market_weather AS w
          ON f.farm_key = w.farm_key AND f.date_key = w.date_key
        GROUP BY w.is_heat_stress""",
}

# Alias every fact column explicitly: with "f.*" ClickHouse prefixes the
# columns that also exist in joined tables (e.g. "f.batch_key").
FACT_BATCH_COLUMNS = ", ".join(f"f.{c} AS {c}" for c in column_names("fact_batch_production"))

BATCH_FEATURES_SQL = f"""
    SELECT {FACT_BATCH_COLUMNS}, b.batch_id, l.line_id, p.sku_id, fm.farm_id, d.full_date,
           d.day_of_week, t.shift_code,
           w.temperature_avg_c, w.humidity_avg_pct, w.thi_index, w.is_heat_stress,
           w.gdt_wmp_usd, w.gdt_smp_usd, w.fao_dairy_index
    FROM {DB}.fact_batch_production AS f
    LEFT JOIN {DB}.dim_batch AS b ON f.batch_key = b.batch_key
    LEFT JOIN {DB}.dim_line AS l ON f.line_key = l.line_key
    LEFT JOIN {DB}.dim_product AS p ON f.product_key = p.product_key
    LEFT JOIN {DB}.dim_farm AS fm ON f.farm_key = fm.farm_key
    LEFT JOIN {DB}.dim_date AS d ON f.date_key = d.date_key
    LEFT JOIN {DB}.dim_time AS t ON f.start_time_key = t.time_key
    LEFT JOIN {DB}.fact_external_market_weather AS w
      ON f.farm_key = w.farm_key AND f.date_key = w.date_key
    ORDER BY d.full_date, b.batch_id"""


class ChdbAnalyticsEngine:
    """Thin wrapper around a persistent chDB session (one per process)."""

    def __init__(self, db_path: str = DEFAULT_DB_PATH):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.sess = chs.Session(db_path)

    def query(self, sql: str, fmt: str = "DataFrame"):
        return self.sess.query(sql, fmt)

    def build(self, gold_dir: str = DEFAULT_GOLD_DIR, from_minio: bool = False) -> None:
        """Load Gold into chDB from local Parquet, or from MinIO when from_minio."""
        gold = Path(gold_dir).resolve()
        self.sess.query(f"CREATE DATABASE IF NOT EXISTS {DB}")
        for table in GOLD_TABLES:
            self.sess.query(f"DROP TABLE IF EXISTS {DB}.{table}")
            self.sess.query(create_table_sql(table, DB))
        for stmt in AGG_MINUTE_DDL:  # MV must exist before the fact insert
            self.sess.query(stmt)
        for table in GOLD_TABLES:
            cols = ", ".join(f"`{c}`" for c in column_names(table))
            if from_minio:
                source = chdb_s3_source(table)
            else:
                src = gold / f"{table}.parquet"
                if not src.exists():
                    print(f"[skip] {src} not found")
                    continue
                source = f"file('{src.as_posix()}', Parquet)"
            try:
                self.sess.query(f"INSERT INTO {DB}.{table} ({cols}) SELECT {cols} FROM {source}")
            except Exception as exc:  # e.g. weather fact not uploaded to MinIO yet
                if not from_minio:
                    raise
                print(f"[skip] {table}: {str(exc).splitlines()[0][:120]}")
        self.sess.query(f"OPTIMIZE TABLE {DB}.agg_sensor_minute FINAL")

    def table_sizes(self):
        return self.query(f"""
            SELECT table, sum(rows) AS rows,
                   formatReadableSize(sum(data_compressed_bytes)) AS compressed,
                   formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed,
                   round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 2) AS ratio
            FROM system.parts WHERE database = '{DB}' AND active
            GROUP BY table ORDER BY table""", "PrettyCompactMonoBlock")

    def run_named(self, name: str, fmt: str = "DataFrame"):
        return self.query(ANALYTICS_QUERIES[name], fmt)

    def batch_features(self):
        """Main hand-off for TV5/TV6: one row per batch with all dimensions."""
        df = self.query(BATCH_FEATURES_SQL)
        df["full_date"] = df["full_date"].dt.date  # pandas turns Date into datetime64
        return df


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--demo", action="store_true")
    parser.add_argument("--export-features", action="store_true")
    parser.add_argument("--gold-dir", default=DEFAULT_GOLD_DIR)
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    parser.add_argument("--from-minio", action="store_true",
                        help="--build reads Gold from MinIO; --export-features also writes to MinIO")
    args = parser.parse_args()

    engine = ChdbAnalyticsEngine(args.db_path)
    if args.build:
        engine.build(args.gold_dir, from_minio=args.from_minio)
        print(engine.table_sizes())
    if args.demo:
        for name in ANALYTICS_QUERIES:
            print(f"\n=== {name}")
            try:
                print(engine.run_named(name, "PrettyCompactMonoBlock"))
            except Exception as exc:  # e.g. weather fact not built yet
                print(f"[error] {exc}")
    if args.export_features:
        out = Path(args.gold_dir) / "batch_features.parquet"
        engine.batch_features().to_parquet(out, index=False)
        print(f"Saved {out}")
        if args.from_minio:  # container has no boto3: let ClickHouse write the object
            engine.sess.query(
                f"INSERT INTO FUNCTION {chdb_s3_source('batch_features')} "
                f"SELECT * FROM file('{out.resolve().as_posix()}', Parquet) "
                "SETTINGS s3_truncate_on_insert = 1"
            )
            print("Saved s3://.../03_gold_marts/batch_features.parquet")


if __name__ == "__main__":
    main()