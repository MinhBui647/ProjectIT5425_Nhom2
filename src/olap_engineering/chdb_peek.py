"""Print every table in factory_analytics.chdb: row count, columns and sample rows (TV4).

    python -m olap_engineering.chdb_peek                 # all tables, 3 rows each
    python -m olap_engineering.chdb_peek dim_farm 10     # one table, 10 rows
"""

import sys

from olap_engineering.chdb_analytics_engine import DB, ChdbAnalyticsEngine


def main():
    table = sys.argv[1] if len(sys.argv) > 1 else None
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else 3
    engine = ChdbAnalyticsEngine()
    tables = [table] if table else engine.query(
        f"SELECT name FROM system.tables WHERE database = '{DB}' AND engine NOT LIKE '%View' ORDER BY name"
    )["name"].tolist()
    for name in tables:
        rows = engine.query(f"SELECT count() AS n FROM {DB}.{name}")["n"][0]
        print(f"\n{'=' * 70}\n BANG {DB}.{name}  -  {rows:,} dong\n{'=' * 70}")
        sql = f"SELECT * FROM {DB}.{name} LIMIT {limit}"
        if name == "agg_sensor_minute":  # binary -State columns: show merged values instead
            sql = (f"SELECT line_key, minute, countMerge(n_readings) AS n_readings, "
                   f"round(avgMerge(uht_temp_avg), 3) AS uht_temp_avg, "
                   f"round(quantileTDigestMerge(0.95)(uht_temp_p95), 3) AS uht_temp_p95, "
                   f"max(uht_temp_max) AS uht_temp_max FROM {DB}.{name} "
                   f"GROUP BY line_key, minute ORDER BY line_key, minute LIMIT {limit}")
        print(engine.query(sql, "PrettyCompactMonoBlock"))


if __name__ == "__main__":
    main()
