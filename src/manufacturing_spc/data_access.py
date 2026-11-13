"""Đọc dữ liệu của TV4 cho TV5: từ chDB hoặc từ file Parquet/CSV xuất ra.

Tên bảng, tên cột lấy đúng theo olap_engineering/star_schema.py (nhánh feature/olap_leading).
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

DB = "dairy"

SENSOR_SQL = f"""
    SELECT batch_key, timestamp, uht_temp, preheat_temp, homo_press_stage1,
           homo_press_stage2, flow_rate, conductivity, power_kw
    FROM {DB}.fact_sensor_telemetry
    WHERE batch_key IS NOT NULL {{extra}}
    ORDER BY batch_key, timestamp"""

# batch_features() của TV4 chỉ có thống kê uht_temp/flow/power theo lô. Áp suất đồng hóa,
# preheat và conductivity chưa có, nên gộp thêm ở đây (TV6, TV7 cần cho What-If Simulator).
BATCH_PROCESS_SQL = f"""
    SELECT batch_key,
           avg(preheat_temp) AS preheat_temp_avg,
           avg(homo_press_stage1) AS homo_press1_avg, stddevSamp(homo_press_stage1) AS homo_press1_std,
           avg(homo_press_stage2) AS homo_press2_avg,
           avg(conductivity) AS conductivity_avg
    FROM {DB}.fact_sensor_telemetry
    WHERE batch_key IS NOT NULL
    GROUP BY batch_key"""


def load_from_chdb(db_path: str | None = None, line_key: int | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Trả về (cảm biến 1 Hz, đặc trưng theo lô đã mở rộng). Cần cài chdb và đã build factory_analytics.chdb."""
    # import trong hàm để máy không có chdb (chạy bằng Parquet) vẫn dùng được các phần khác
    from olap_engineering.chdb_analytics_engine import DEFAULT_DB_PATH, ChdbAnalyticsEngine

    eng = ChdbAnalyticsEngine(db_path or DEFAULT_DB_PATH)
    extra = f"AND line_key = {int(line_key)}" if line_key is not None else ""
    sensor = eng.query(SENSOR_SQL.format(extra=extra))
    batches = eng.batch_features().merge(eng.query(BATCH_PROCESS_SQL), on="batch_key", how="left")
    return sensor, batches


def _read(path: str | Path) -> pd.DataFrame:
    path = Path(path)
    if path.suffix == ".csv":
        # file cảm biến có cột timestamp thì parse luôn thành thời gian
        head = pd.read_csv(path, nrows=0).columns
        return pd.read_csv(path, parse_dates=["timestamp"] if "timestamp" in head else None)
    return pd.read_parquet(path)


def load_from_parquet(sensor_path: str | Path, batch_features_path: str | Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Không cần chdb hay Docker: đọc thẳng file Gold TV4 xuất ra (data/03_gold_marts/).

    - sensor_path: fact_sensor_telemetry.parquet
    - batch_features_path: batch_features.parquet (TV4 tạo bằng chdb_analytics_engine --export-features)
    Thống kê áp suất đồng hóa theo lô được gộp bằng pandas, tương đương BATCH_PROCESS_SQL ở trên.
    """
    sensor, batches = _read(sensor_path), _read(batch_features_path)
    sensor = sensor[sensor["batch_key"].notna()].copy()
    sensor["batch_key"] = sensor["batch_key"].astype("int64")
    agg = {c: (c, "mean") for c in ("preheat_temp", "homo_press_stage1", "homo_press_stage2", "conductivity")
           if c in sensor.columns}  # cột nào TV4 không xuất thì bỏ qua
    if agg:
        g = sensor.groupby("batch_key").agg(**{f"{k}_avg": v for k, v in agg.items()})
        g = g.rename(columns={"homo_press_stage1_avg": "homo_press1_avg", "homo_press_stage2_avg": "homo_press2_avg"})
        batches = batches.merge(g.reset_index(), on="batch_key", how="left")
    return sensor, batches
