"""8 quy tắc Nelson (Western Electric mở rộng) áp lên chuỗi X̄ (TV5)."""

from __future__ import annotations

import numpy as np
import pandas as pd

RULE_DESCRIPTIONS = {
    1: "1 điểm vượt 3 sigma",
    2: "9 điểm liên tiếp cùng một phía đường tâm",
    3: "6 điểm liên tiếp tăng hoặc giảm đều",
    4: "14 điểm liên tiếp lên xuống xen kẽ",
    5: "2/3 điểm vượt 2 sigma cùng phía",
    6: "4/5 điểm vượt 1 sigma cùng phía",
    7: "15 điểm liên tiếp trong vùng ±1 sigma",
    8: "8 điểm liên tiếp ngoài vùng ±1 sigma (cả hai phía)",
}


def _all_true(mask: np.ndarray, k: int) -> np.ndarray:
    """True tại vị trí i nếu k phần tử mask[i-k+1 .. i] đều True."""
    return pd.Series(mask).rolling(k).sum().eq(k).to_numpy()


def _at_least(mask: np.ndarray, k: int, window: int) -> np.ndarray:
    """True tại i nếu trong `window` phần tử gần nhất có ít nhất k phần tử True."""
    return pd.Series(mask).rolling(window).sum().ge(k).to_numpy()


def nelson_flags(x: np.ndarray, center: float, sigma: float) -> pd.DataFrame:
    """Trả về bảng bool (rule_1 … rule_8). Cờ gắn ở điểm hoàn thành mẫu hình."""
    x = np.asarray(x, float)
    z = (x - center) / sigma
    d = np.diff(x, prepend=np.nan)  # hiệu liên tiếp, d[0] là NaN nên so sánh luôn cho False

    flags = {}
    flags[1] = np.abs(z) > 3
    side = np.sign(z)
    flags[2] = _all_true(side > 0, 9) | _all_true(side < 0, 9)
    # 6 điểm tăng/giảm đều ⇒ cần 5 hiệu liên tiếp cùng dấu
    flags[3] = _all_true(d > 0, 5) | _all_true(d < 0, 5)
    # 14 điểm xen kẽ ⇒ 13 hiệu, tức 12 cặp hiệu liên tiếp trái dấu nhau
    alt = np.zeros(len(x), bool)
    alt[1:] = d[1:] * d[:-1] < 0
    flags[4] = _all_true(alt, 12)
    # quy tắc 5, 6: điểm hiện tại cũng phải nằm ngoài ngưỡng, kẻo gắn cờ nhầm vào điểm "đứng cạnh"
    flags[5] = (_at_least(z > 2, 2, 3) & (z > 2)) | (_at_least(z < -2, 2, 3) & (z < -2))
    flags[6] = (_at_least(z > 1, 4, 5) & (z > 1)) | (_at_least(z < -1, 4, 5) & (z < -1))
    flags[7] = _all_true(np.abs(z) < 1, 15)
    flags[8] = _all_true(np.abs(z) > 1, 8)

    out = pd.DataFrame({f"rule_{r}": np.nan_to_num(v, nan=0).astype(bool) for r, v in flags.items()})
    out["any_violation"] = out.any(axis=1)
    return out


def violation_log(df: pd.DataFrame, flags: pd.DataFrame, value_col: str, chart: str,
                  id_col: str | None = None) -> pd.DataFrame:
    """Nhật ký vi phạm dạng dài cho TV7: mỗi dòng là một điểm vi phạm một quy tắc.

    `df` cần có cột batch_key và timestamp; `chart` ghi biểu đồ nào ("xbar_minute" hoặc "I_batch").
    """
    rows = []
    keep = [c for c in (id_col, "timestamp", "batch_key", value_col) if c]
    for r, text in RULE_DESCRIPTIONS.items():
        idx = np.flatnonzero(flags[f"rule_{r}"].to_numpy())
        if len(idx):
            part = df.iloc[idx][keep].copy()
            part = part.rename(columns={value_col: "value"})
            part["chart"] = chart
            part["rule"] = r
            part["description"] = text
            rows.append(part)
    cols = [c for c in (id_col, "timestamp", "batch_key", "value", "chart", "rule", "description") if c]
    return pd.concat(rows, ignore_index=True)[cols] if rows else pd.DataFrame(columns=cols)


def expected_false_alarm_rates(n_sim: int = 1_000_000, seed: int = 0) -> dict[int, float]:
    """Tỉ lệ báo động giả KỲ VỌNG của từng quy tắc (trên mỗi điểm) khi quy trình ổn định.

    Mô phỏng chuỗi N(0,1) độc lập rồi đếm cờ bằng đúng định nghĩa ở trên, nên so sánh với số vi phạm
    thực tế là cùng một thước đo. Vài giá trị đối chiếu với lý thuyết: quy tắc 1 ≈ 0,0027;
    quy tắc 3 = 2/6! ≈ 0,0028; quy tắc 7 = 0,6827^15 ≈ 0,0033.
    """
    z = np.random.default_rng(seed).normal(0.0, 1.0, n_sim)
    f = nelson_flags(z, 0.0, 1.0)
    return {r: float(f[f"rule_{r}"].mean()) for r in RULE_DESCRIPTIONS}


def compare_with_expected(flags: pd.DataFrame, expected: dict[int, float], chart: str, phase: str) -> pd.DataFrame:
    """So số vi phạm thực tế với số báo động giả kỳ vọng theo lý thuyết.

    p-value Poisson chỉ mang tính tham khảo: các cửa sổ chồng lấn nên các lần báo không độc lập hẳn.
    """
    from scipy import stats

    n = len(flags)
    rows = []
    for r, text in RULE_DESCRIPTIONS.items():
        obs = int(flags[f"rule_{r}"].sum())
        exp = expected[r] * n
        rows.append({
            "chart": chart, "phase": phase, "n_points": n, "rule": r, "description": text,
            "observed": obs, "expected_false_alarms": round(exp, 2),
            "ratio_obs_over_exp": (obs / exp) if exp > 0 else np.nan,
            "poisson_p_ge_observed": float(stats.poisson.sf(obs - 1, exp)) if exp > 0 else np.nan,
        })
    return pd.DataFrame(rows)
