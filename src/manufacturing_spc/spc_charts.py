"""Biểu đồ kiểm soát Shewhart X̄–S, CUSUM và EWMA (TV5)."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from math import gamma, sqrt

import numpy as np
import pandas as pd


def c4(n: int) -> float:
    """Hệ số hiệu chỉnh độ lệch chuẩn nhóm con: E[S] = c4(n) · sigma."""
    if n < 2:
        raise ValueError("n phải >= 2")
    return sqrt(2.0 / (n - 1)) * gamma(n / 2) / gamma((n - 1) / 2)


def xbar_s_constants(n: int) -> dict[str, float]:
    """A3, B3, B4 tính thẳng từ c4 nên không phải tra bảng (n=5 ra A3=1,427; B4=2,089)."""
    k = c4(n)
    spread = 3.0 * sqrt(1.0 - k * k) / k
    return {"c4": k, "A3": 3.0 / (k * sqrt(n)), "B3": max(0.0, 1.0 - spread), "B4": 1.0 + spread}


@dataclass
class XbarSLimits:
    n: int
    xbar_cl: float       # X̿ (trung bình của các trung bình nhóm con)
    xbar_ucl: float
    xbar_lcl: float
    s_cl: float          # S̄
    s_ucl: float
    s_lcl: float
    sigma_within: float  # S̄ / c4: độ biến thiên ngắn hạn trong nhóm con
    sigma_xbar: float    # sigma_within / √n: dùng làm "1 sigma" cho Nelson, CUSUM, EWMA
    n_subgroups_used: int

    def to_dict(self) -> dict:
        return asdict(self)


def make_subgroups(
    df: pd.DataFrame,
    value: str = "uht_temp",
    n: int = 5,
    every_s: int = 60,
    group: str = "batch_key",
    time: str = "timestamp",
) -> pd.DataFrame:
    """Chia nhóm con hợp lý (rational subgrouping) từ dữ liệu 1 giây/mẫu.

    Trong từng lô, cứ mỗi `every_s` giây lấy `n` mẫu liên tiếp đầu tiên làm một nhóm con.
    Không vẽ thẳng từng giây vì dữ liệu cảm biến thường tự tương quan, vẽ như vậy sẽ báo
    động giả rất nhiều.
    """
    d = df[[group, time, value]].dropna().sort_values([group, time])
    pos = d.groupby(group).cumcount()  # thứ tự mẫu trong lô (0, 1, 2, ...)
    d = d.assign(_sg=pos // every_s, _pos=pos % every_s)
    d = d[d["_pos"] < n]  # chỉ giữ n mẫu đầu của mỗi chu kỳ
    out = (
        d.groupby([group, "_sg"], sort=False)
        .agg(timestamp=(time, "first"), xbar=(value, "mean"), s=(value, "std"), n=(value, "size"))
        .reset_index()
        .rename(columns={"_sg": "subgroup_in_batch"})
    )
    out = out[out["n"] == n].sort_values("timestamp").reset_index(drop=True)  # bỏ nhóm con thiếu mẫu
    out.insert(0, "subgroup_id", np.arange(len(out)))
    return out


def _limits(xbar: np.ndarray, s: np.ndarray, n: int) -> XbarSLimits:
    k = xbar_s_constants(n)
    xbb, sbar = float(np.mean(xbar)), float(np.mean(s))
    sigma_w = sbar / k["c4"]
    return XbarSLimits(
        n=n, xbar_cl=xbb, xbar_ucl=xbb + k["A3"] * sbar, xbar_lcl=xbb - k["A3"] * sbar,
        s_cl=sbar, s_ucl=k["B4"] * sbar, s_lcl=k["B3"] * sbar,
        sigma_within=sigma_w, sigma_xbar=sigma_w / sqrt(n), n_subgroups_used=len(xbar),
    )


def phase1_limits(sg: pd.DataFrame, n: int, *, phase1_mask=None, fraction: float = 0.25,
                  max_iter: int = 10) -> XbarSLimits:
    """Phase I: tính giới hạn từ giai đoạn baseline, loại nhóm con ngoài kiểm soát rồi tính lại.

    `phase1_mask` là mặt nạ bool chọn nhóm con thuộc Phase I (ví dụ tháng 01/2025). Không truyền
    thì dùng `fraction` đầu của chuỗi. Đừng tính giới hạn trên toàn bộ dữ liệu: những lệch chuẩn
    mình muốn phát hiện sẽ lọt vào giới hạn và làm nó rộng ra.
    """
    if phase1_mask is not None:
        base = sg[np.asarray(phase1_mask)]
    else:
        base = sg.iloc[: max(int(len(sg) * fraction), 25)]
    xbar, s = base["xbar"].to_numpy(), base["s"].to_numpy()
    for _ in range(max_iter):
        lim = _limits(xbar, s, n)
        keep = (xbar <= lim.xbar_ucl) & (xbar >= lim.xbar_lcl) & (s <= lim.s_ucl) & (s >= lim.s_lcl)
        # dừng khi không còn điểm nào vượt giới hạn (hoặc loại nữa thì còn quá ít điểm)
        if keep.all() or keep.sum() < 20:
            return lim
        xbar, s = xbar[keep], s[keep]
    return _limits(xbar, s, n)


def cusum(x: np.ndarray, mu0: float, sigma: float, k: float = 0.5, h: float = 5.0, reset: bool = True) -> pd.DataFrame:
    """CUSUM hai phía dạng chuẩn hóa (đơn vị sigma). Bắt tốt các lệch nhỏ 0,5–1,5 sigma."""
    z = (np.asarray(x, float) - mu0) / sigma
    cp, cm = np.zeros(len(z)), np.zeros(len(z))
    sig_up, sig_dn = np.zeros(len(z), bool), np.zeros(len(z), bool)
    p = m = 0.0
    for i, zi in enumerate(z):
        # tích lũy độ lệch vượt quá k, không cho âm (max với 0)
        p, m = max(0.0, p + zi - k), max(0.0, m - zi - k)
        cp[i], cm[i] = p, m
        if p > h:
            sig_up[i] = True
            if reset:
                p = 0.0  # báo xong thì đặt lại để theo dõi tiếp
        if m > h:
            sig_dn[i] = True
            if reset:
                m = 0.0
    return pd.DataFrame({"c_plus": cp, "c_minus": cm, "signal_up": sig_up, "signal_down": sig_dn})


def ewma(x: np.ndarray, mu0: float, sigma: float, lam: float = 0.2, L: float = 3.0) -> pd.DataFrame:
    """EWMA: Z_i = λ·X_i + (1−λ)·Z_{i−1}, bắt đầu Z_0 = mu0. Giới hạn rộng dần theo i."""
    x = np.asarray(x, float)
    zs = np.empty(len(x))
    prev = mu0
    for i, xi in enumerate(x):
        prev = lam * xi + (1 - lam) * prev
        zs[i] = prev
    i = np.arange(1, len(x) + 1)
    # mấy điểm đầu giới hạn còn hẹp, sau đó tiến dần đến mức ổn định L·sigma·√(λ/(2−λ))
    width = L * sigma * np.sqrt(lam / (2 - lam) * (1 - (1 - lam) ** (2 * i)))
    return pd.DataFrame({"z": zs, "ucl": mu0 + width, "lcl": mu0 - width,
                         "signal": (zs > mu0 + width) | (zs < mu0 - width)})


@dataclass
class ImrLimits:
    """Giới hạn biểu đồ I-MR (cá thể và khoảng trượt) cho chuỗi trung bình từng lô."""
    x_cl: float
    x_ucl: float
    x_lcl: float
    mr_cl: float
    mr_ucl: float
    mr_lcl: float
    sigma: float  # MR̄ / d2: ước lượng sigma của từng điểm, dùng làm "1 sigma" cho Nelson/CUSUM/EWMA
    n_points_used: int

    def to_dict(self) -> dict:
        return asdict(self)


def imr_limits(x: np.ndarray, d2: float = 1.128, d4: float = 3.267, max_iter: int = 10) -> ImrLimits:
    """Giới hạn I-MR: UCL/LCL = X̄ ± 3·MR̄/d2, MR: UCL = D4·MR̄, LCL = 0. Loại điểm ngoài giới hạn rồi tính lại."""
    x = np.asarray(x, float)
    for _ in range(max_iter):
        mr = np.abs(np.diff(x))
        cl, mrbar = float(x.mean()), float(mr.mean())
        sigma = mrbar / d2
        keep = (x <= cl + 3 * sigma) & (x >= cl - 3 * sigma)
        if keep.all() or keep.sum() < 20:
            break
        x = x[keep]  # bỏ điểm ngoài kiểm soát (khoảng trượt mới tính lại trên chuỗi đã lọc)
    mr = np.abs(np.diff(x))
    cl, mrbar = float(x.mean()), float(mr.mean())
    sigma = mrbar / d2
    return ImrLimits(cl, cl + 3 * sigma, cl - 3 * sigma, mrbar, d4 * mrbar, 0.0, sigma, len(x))


def batch_means(df: pd.DataFrame, value: str = "uht_temp", group: str = "batch_key", time: str = "timestamp") -> pd.DataFrame:
    """Trung bình từng lô (mỗi lô một điểm) kèm thời điểm bắt đầu, tháng và số mẫu. Sắp theo thời gian."""
    g = (df.dropna(subset=[value]).groupby(group)
         .agg(timestamp=(time, "min"), mean=(value, "mean"), std=(value, "std"), n=(value, "size"))
         .reset_index().sort_values("timestamp").reset_index(drop=True))
    g["month"] = pd.to_datetime(g["timestamp"]).dt.month
    g.insert(0, "batch_order", np.arange(len(g)))
    return g
