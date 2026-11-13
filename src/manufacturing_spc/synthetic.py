"""Dữ liệu GIẢ LẬP để chạy thử code khi chưa có dữ liệu thật của TV4 (TV5).

Chỉ dùng để kiểm tra code chạy được, đừng đưa số liệu này vào báo cáo như dữ liệu thật.
Cấu trúc giống dữ liệu thật: 4 lô/ngày bắt đầu từ 01/01/2025 (240 lô trải khoảng 60 ngày, tức tháng 01–02
và 1 ngày đầu tháng 03), mỗi lô 7200 mẫu 1 Hz. Khác bộ sinh của TV1: mỗi lô có seed riêng và có thể chèn sẵn
một lệch mức ở Phase II để SPC có cái để bắt.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def batch_start(b: int) -> pd.Timestamp:
    """Thời điểm bắt đầu lô thứ b: 4 lô mỗi ngày, cách nhau 6 giờ."""
    return pd.Timestamp("2025-01-01 01:00:00", tz="UTC") + pd.Timedelta(days=b // 4, hours=6 * (b % 4))


def make_demo_sensor(n_batches: int = 240, samples: int = 7200, seed: int = 5425,
                     shift_from_batch: int | None = 190, shift_sigma: float = 1.0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    parts = []
    for b in range(n_batches):
        drift = rng.normal(0, 0.1)  # mỗi lô lệch nhẹ so với mức chung
        # từ lô shift_from_batch trở đi nhiệt độ UHT bị đẩy lên một mức cố định (nằm trong Phase II)
        level = 140.0 + drift + (shift_sigma * 0.8 if shift_from_batch is not None and b >= shift_from_batch else 0.0)
        parts.append(pd.DataFrame({
            "batch_key": b + 1,
            "timestamp": batch_start(b) + pd.to_timedelta(np.arange(samples), unit="s"),
            "uht_temp": rng.normal(level, 0.8, samples),
            "homo_press_stage1": rng.normal(180 + drift, 3.0, samples),
        }))
    return pd.concat(parts, ignore_index=True)


def make_demo_batches(n_batches: int = 240, seed: int = 7, sensor: pd.DataFrame | None = None) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    thi = rng.normal(70, 4, n_batches)
    # THI cao thì hàm lượng béo giảm nhẹ (giống ý tưởng stress nhiệt ở bò sữa)
    fat = rng.normal(3.5, 0.1, n_batches) - 0.01 * np.clip(thi - 72, 0, None)
    uht = rng.normal(140, 0.1, n_batches)
    if sensor is not None:  # lấy trung bình thật từ cảm biến cho nhất quán với biểu đồ I
        m = sensor.groupby("batch_key")["uht_temp"].mean().reindex(np.arange(1, n_batches + 1)).to_numpy()
        uht = np.where(np.isnan(m), uht, m)
    b = np.arange(n_batches)
    return pd.DataFrame({
        "batch_key": b + 1,
        "sku_id": np.array(["SKU_NGUYEN_CHAT", "SKU_CO_DUONG", "SKU_IT_BEO"])[b % 3],
        "shift_code": np.array(["Sáng", "Chiều", "Tối", "Đêm"])[b % 4],
        "farm_id": np.array(["MOC_CHAU", "BA_VI", "NGHE_AN", "LAM_DONG", "CU_CHI"])[(b // 2) % 5],
        "fat_in": fat, "protein_in": rng.normal(3.2, 0.05, n_batches),
        "acidity_sh": rng.normal(6.5, 0.2, n_batches), "thi_index": thi,
        "is_heat_stress": (thi > 72).astype(int),
        "uht_temp_avg": uht, "uht_temp_std": rng.normal(0.8, 0.02, n_batches),
        "uht_temp_min": uht - 3.3, "uht_temp_max": uht + 3.3,
        "n_readings": 7200,  # cố ý để hằng số, kiểm tra bước loại biến hằng
        "yield_recovery_pct": 95 + 8 * (fat - 3.5) - 0.5 * (uht - 140) + rng.normal(0, 1.0, n_batches),
        "batch_defect_flag": (rng.random(n_batches) < 0.05).astype(int),
    })
