"""Kiểm thử phát hiện: chèn các kịch bản bất thường vào chuỗi SPC rồi xem biểu đồ nào bắt được (TV5).

Có hai bản:
- detection_on_batches: ĐÚNG THEO TÀI LIỆU NHIỆM VỤ. Cấy lệch +1,5σ, +0,5σ và trôi dần vào chuỗi trung bình
  từng lô thật (Phase II), ghi lô đầu tiên mà Shewhart, CUSUM, EWMA phát cảnh báo.
- detection_montecarlo: bản mô phỏng mở rộng (nhiều lần lặp, 8 kịch bản) để có thống kê ổn định hơn.

Ý tưởng: lấy giới hạn Phase I thật của pipeline (trung bình, sigma_within, S̄...) để mô phỏng
chuỗi nhóm con "bình thường", rồi từ nhóm con số `inject_at` chèn một bất thường đã biết trước:
lệch mức, trôi dần, đột biến một điểm hoặc tăng độ phân tán. Với mỗi (kịch bản, phương pháp)
ghi lại tỉ lệ phát hiện, độ trễ phát hiện (tính bằng số nhóm con) và tỉ lệ báo động giả trước
thời điểm chèn. Kịch bản "không bất thường" dùng để đo báo động giả thuần túy.

Cách đọc kết quả:
- detect_rate: có tín hiệu bất kỳ lúc nào từ điểm chèn đến hết chuỗi. Với bất thường kéo dài (lệch
  mức, trôi dần, tăng phân tán) thì con số này phản ánh đúng khả năng phát hiện. Với bất thường
  thoáng qua (đột biến một điểm) hay kịch bản không bất thường, nó bị lẫn cả báo động giả xảy ra
  sau đó, nên hãy xem detect_within_10_rate.
- detect_within_10_rate: có tín hiệu trong 10 nhóm con ngay sau điểm chèn (đo bắt "kịp thời").
- s_chart chỉ nhắm vào thay đổi độ phân tán, ở các kịch bản khác nó không phải công cụ phù hợp.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from . import config as cfg
from .nelson_rules import nelson_flags
from .spc_charts import XbarSLimits, cusum, ewma

# (tên kịch bản, mô tả, hàm biến đổi (xbar, s, vị trí chèn, sigma_xbar) -> (xbar, s))
def _shift(k: float):
    def f(xbar, s, t0, sx):
        xbar = xbar.copy()
        xbar[t0:] += k * sx
        return xbar, s
    return f


def _drift(per_step: float):
    def f(xbar, s, t0, sx):
        xbar = xbar.copy()
        steps = np.arange(1, len(xbar) - t0 + 1)
        xbar[t0:] += per_step * steps * sx  # trôi tuyến tính, mỗi nhóm con thêm per_step sigma
        return xbar, s
    return f


def _spike(k: float):
    def f(xbar, s, t0, sx):
        xbar = xbar.copy()
        xbar[t0] += k * sx  # chỉ một điểm duy nhất
        return xbar, s
    return f


def _variance(factor: float, lim: XbarSLimits):
    def f(xbar, s, t0, sx):
        # độ phân tán tăng gấp `factor` lần: các trung bình nhóm con tản ra hơn và s cũng to lên
        xbar = xbar.copy()
        xbar[t0:] = lim.xbar_cl + (xbar[t0:] - lim.xbar_cl) * factor
        s = s.copy()
        s[t0:] *= factor
        return xbar, s
    return f


def scenarios(lim: XbarSLimits) -> list[tuple[str, str, callable]]:
    return [
        ("khong_bat_thuong", "Quy trình ổn định, dùng để đo báo động giả", lambda x, s, t0, sx: (x, s)),
        ("lech_muc_0.5sigma", "Giá trị trung bình lệch +0,5 sigma từ điểm chèn", _shift(0.5)),
        ("lech_muc_1.0sigma", "Giá trị trung bình lệch +1,0 sigma từ điểm chèn", _shift(1.0)),
        ("lech_muc_1.5sigma", "Giá trị trung bình lệch +1,5 sigma từ điểm chèn", _shift(1.5)),
        ("lech_muc_2.0sigma", "Giá trị trung bình lệch +2,0 sigma từ điểm chèn", _shift(2.0)),
        ("troi_dan", "Trôi tuyến tính +0,03 sigma mỗi nhóm con (giống đóng cặn dần)", _drift(0.03)),
        ("dot_bien_4sigma", "Một điểm đột biến +4 sigma rồi trở lại bình thường", _spike(4.0)),
        ("tang_phan_tan_1.5x", "Độ phân tán tăng 1,5 lần (van hoặc cảm biến rung)", _variance(1.5, lim)),
    ]


def _first_signal(mask: np.ndarray, start: int) -> int | None:
    """Vị trí tín hiệu đầu tiên từ `start` trở đi, không có thì trả None."""
    idx = np.flatnonzero(mask[start:])
    return int(idx[0]) + start if len(idx) else None


def detection_montecarlo(lim: XbarSLimits, n_rep: int = 200, horizon: int = 400, inject_at: int = 100,
                        seed: int = 0) -> pd.DataFrame:
    """Chạy các kịch bản bằng Monte Carlo. `horizon` là độ dài chuỗi, bất thường chèn tại `inject_at`."""
    rng = np.random.default_rng(seed)
    n = lim.n
    sx = lim.sigma_xbar
    rows = []
    for name, desc, apply in scenarios(lim):
        # kết quả theo từng phương pháp: (đã phát hiện?, độ trễ, báo động giả trước điểm chèn?, phát hiện trong 10 nhóm?)
        res: dict[str, list] = {m: [] for m in ("shewhart_3sigma", "shewhart_nelson", "cusum", "ewma", "s_chart")}
        for _ in range(n_rep):
            xbar = rng.normal(lim.xbar_cl, sx, horizon)
            # s của nhóm con cỡ n: sigma·√(χ²_{n−1}/(n−1))
            s = lim.sigma_within * np.sqrt(rng.chisquare(n - 1, horizon) / (n - 1))
            xbar, s = apply(xbar, s, inject_at, sx)

            masks = {
                "shewhart_3sigma": (xbar > lim.xbar_ucl) | (xbar < lim.xbar_lcl),
                "shewhart_nelson": nelson_flags(xbar, lim.xbar_cl, sx)["any_violation"].to_numpy(),
                "s_chart": (s > lim.s_ucl) | (s < lim.s_lcl),
            }
            cu = cusum(xbar, lim.xbar_cl, sx, cfg.CUSUM_K, cfg.CUSUM_H)
            masks["cusum"] = (cu["signal_up"] | cu["signal_down"]).to_numpy()
            masks["ewma"] = ewma(xbar, lim.xbar_cl, sx, cfg.EWMA_LAMBDA, cfg.EWMA_L)["signal"].to_numpy()

            for method, mask in masks.items():
                first = _first_signal(mask, inject_at)
                delay = (first - inject_at + 1) if first is not None else np.nan
                res[method].append((first is not None, delay, bool(mask[:inject_at].any()),
                                    first is not None and delay <= 10))
        for method, vals in res.items():
            det = np.array([v[0] for v in vals])
            delay = np.array([v[1] for v in vals], float)
            rows.append({
                "scenario": name, "description": desc, "method": method, "n_rep": n_rep,
                "inject_at": inject_at, "horizon": horizon,
                "detect_rate": float(det.mean()),
                "detect_within_10_rate": float(np.mean([v[3] for v in vals])),
                "mean_delay_subgroups": float(np.nanmean(delay)) if det.any() else np.nan,
                "median_delay_subgroups": float(np.nanmedian(delay)) if det.any() else np.nan,
                "false_alarm_before_rate": float(np.mean([v[2] for v in vals])),
            })
    return pd.DataFrame(rows)


def detection_on_batches(batch_df: pd.DataFrame, imr, inject_at: int = 20, drift_per_batch: float = 0.05) -> pd.DataFrame:
    """Cấy kịch bản vào chuỗi trung bình lô (Phase II) rồi ghi lô đầu tiên bị mỗi phương pháp phát hiện.

    batch_df: các lô Phase II theo thứ tự thời gian, có cột batch_key, mean.
    imr: giới hạn I-MR đã ước lượng ở Phase I (ImrLimits).
    inject_at: vị trí lô (trong Phase II) bắt đầu cấy sự cố, để trước đó có một đoạn bình thường.
    drift_per_batch: độ trôi mỗi lô, tính theo sigma (0,05σ/lô thì sau 40 lô đã trôi 2σ).
    """
    x0 = batch_df["mean"].to_numpy(float)
    ids = batch_df["batch_key"].to_numpy()
    sigma = imr.sigma
    steps = np.arange(1, len(x0) - inject_at + 1)

    plans = [
        ("doi_chung_khong_cay", "Không cấy gì: dùng chuỗi thật để xem báo động nền", np.zeros(len(x0))),
        ("dich_+1.5sigma", "Cấy lệch trung bình +1,5σ từ lô inject_at", np.r_[np.zeros(inject_at), np.full(len(steps), 1.5 * sigma)]),
        ("dich_+0.5sigma", "Cấy lệch trung bình +0,5σ từ lô inject_at", np.r_[np.zeros(inject_at), np.full(len(steps), 0.5 * sigma)]),
        ("troi_dan_drift", f"Cấy trôi tuyến tính +{drift_per_batch}σ mỗi lô từ lô inject_at",
         np.r_[np.zeros(inject_at), drift_per_batch * sigma * steps]),
    ]
    rows = []
    for name, desc, add in plans:
        x = x0 + add
        cu = cusum(x, imr.x_cl, sigma, cfg.CUSUM_K, cfg.CUSUM_H)
        masks = {
            "shewhart_I": (x > imr.x_ucl) | (x < imr.x_lcl),
            "shewhart_nelson": nelson_flags(x, imr.x_cl, sigma)["any_violation"].to_numpy(),
            "cusum": (cu["signal_up"] | cu["signal_down"]).to_numpy(),
            "ewma": ewma(x, imr.x_cl, sigma, cfg.EWMA_LAMBDA, cfg.EWMA_L)["signal"].to_numpy(),
        }
        for method, mask in masks.items():
            first = _first_signal(mask, inject_at)
            rows.append({
                "scenario": name, "description": desc, "method": method,
                "inject_batch_order": inject_at, "inject_batch_id": ids[inject_at],
                "detected": first is not None,
                "first_detect_batch_order": first if first is not None else np.nan,
                "first_detect_batch_id": ids[first] if first is not None else np.nan,
                "delay_batches": (first - inject_at + 1) if first is not None else np.nan,
                "false_alarm_before_injection": bool(mask[:inject_at].any()),
                "n_batches_phase2": len(x0),
            })
    return pd.DataFrame(rows)
