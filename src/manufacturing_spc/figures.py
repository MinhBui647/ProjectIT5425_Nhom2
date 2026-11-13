"""Vẽ các hình bàn giao của TV5 (matplotlib, lưu PNG vào reports/spc/figures/).

Quy chuẩn màu của nhóm: bình thường #184C78, cảnh báo #D97706, vi phạm #D9534F.
Không dùng bảng màu cầu vồng; lưới nền nhạt; ghi nhãn trực tiếp trên đường giới hạn.
"""

from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # vẽ ra file, không cần cửa sổ hiển thị
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from matplotlib.colors import LinearSegmentedColormap  # noqa: E402
from scipy import stats  # noqa: E402

NORMAL, WARN, ALARM, GRID, GREY = "#184C78", "#D97706", "#D9534F", "#E5E7EB", "#6B7280"
DIVERGING = LinearSegmentedColormap.from_list("tv5", [ALARM, "#FFFFFF", NORMAL])  # đỏ – trắng – xanh thép
DPI = 130


def _style(ax) -> None:
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    ax.grid(axis="y", color=GRID, linewidth=0.7)
    ax.set_axisbelow(True)


def _hline(ax, y: float, label: str, color: str = ALARM, ls: str = "--") -> None:
    ax.axhline(y, color=color, ls=ls, lw=1)
    # nhãn gắn thẳng vào đầu bên phải đường giới hạn, người xem không phải tra chú giải
    ax.annotate(f"{label} {y:.2f}", xy=(1.005, y), xycoords=("axes fraction", "data"),
                color=color, fontsize=8, va="center")


def _phase_line(ax, x_boundary: float | None) -> None:
    if x_boundary is None:
        return
    ax.axvline(x_boundary, color=GREY, lw=1, ls=":")
    ymax = ax.get_ylim()[1]
    ax.text(x_boundary, ymax, " Phase II →", color=GREY, fontsize=8, va="top")
    ax.text(x_boundary, ymax, "← Phase I ", color=GREY, fontsize=8, va="top", ha="right")


def _save(fig, path: Path) -> Path:
    fig.tight_layout()
    fig.savefig(path, dpi=DPI, bbox_inches="tight")
    plt.close(fig)
    return path


def fig_hist_qq(sensor: pd.DataFrame, out: Path, var: str = "uht_temp") -> Path:
    v = sensor[var].dropna().to_numpy(float)
    mu, sd = v.mean(), v.std(ddof=1)
    n3 = int((np.abs(v - mu) > 3 * sd).sum())
    q1, q3 = np.percentile(v, [25, 75])
    niqr = int(((v < q1 - 1.5 * (q3 - q1)) | (v > q3 + 1.5 * (q3 - q1))).sum())
    fig, (a, b) = plt.subplots(1, 2, figsize=(11, 4))
    a.hist(v, bins=100, density=True, color=NORMAL, alpha=0.8)
    xs = np.linspace(v.min(), v.max(), 300)
    a.plot(xs, stats.norm.pdf(xs, mu, sd), color=WARN, lw=2)
    a.set_title(f"Phân phối {var} (n = {len(v):,})")
    a.set_xlabel("°C")
    a.text(0.02, 0.95, f"Ngoại lai ±3σ: {n3:,}\nNgoại lai IQR: {niqr:,}", transform=a.transAxes, va="top", fontsize=9)
    sample = v if len(v) <= 20000 else np.random.default_rng(0).choice(v, 20000, replace=False)
    (osm, osr), (slope, intercept, r) = stats.probplot(sample, dist="norm")
    b.scatter(osm, osr, s=3, color=NORMAL, alpha=0.5)
    b.plot(osm, slope * osm + intercept, color=ALARM, lw=1.2)
    b.set_title(f"Q-Q plot so với phân phối chuẩn (r = {r:.4f})")
    b.set_xlabel("Phân vị lý thuyết")
    b.set_ylabel("Phân vị mẫu (°C)")
    for ax in (a, b):
        _style(ax)
    return _save(fig, out / "fig01_uht_hist_qq.png")


def group_columns(batches: pd.DataFrame) -> dict[str, str]:
    """Chọn cột nhóm có thật trong batch_features cho từng tiêu chí (sản phẩm, ca, trang trại)."""
    from .config import GROUP_CANDIDATES
    found = {}
    for label, cands in GROUP_CANDIDATES.items():
        for c in cands:
            if c in batches.columns:
                found[label] = c
                break
    return found


def fig_boxplots(batches: pd.DataFrame, out: Path) -> Path | None:
    groups = group_columns(batches)
    ys = [c for c in ("uht_temp_avg", "yield_recovery_pct") if c in batches.columns]
    if not groups or not ys:
        return None
    fig, axes = plt.subplots(len(ys), len(groups), figsize=(4.2 * len(groups), 3.6 * len(ys)), squeeze=False)
    for i, y in enumerate(ys):
        for j, (label, col) in enumerate(groups.items()):
            ax = axes[i][j]
            cats = sorted(batches[col].dropna().unique())
            data = [batches.loc[batches[col] == c, y].dropna() for c in cats]
            ax.boxplot(data, tick_labels=[str(c) for c in cats], patch_artist=True, widths=0.6,
                       boxprops=dict(facecolor=NORMAL, alpha=0.35, edgecolor=NORMAL), medianprops=dict(color=WARN, lw=2),
                       flierprops=dict(marker="o", markersize=3, markeredgecolor=ALARM))
            ax.set_title(f"{y} theo {label}")
            ax.tick_params(axis="x", labelrotation=30)
            _style(ax)
    return _save(fig, out / "fig02_boxplots_nhom.png")


def fig_xbar_s(sg: pd.DataFrame, flags: pd.DataFrame, lim, p2_start: int | None, out: Path) -> Path:
    fig, (a, b) = plt.subplots(2, 1, figsize=(12, 7), sharex=True)
    x = np.arange(len(sg))
    bad = flags["any_violation"].to_numpy()
    a.plot(x, sg["xbar"], color=NORMAL, lw=0.4)
    a.scatter(x[bad], sg["xbar"].to_numpy()[bad], s=6, color=ALARM, zorder=3)
    for val, lab, col, ls in ((lim.xbar_ucl, "UCL", ALARM, "--"), (lim.xbar_cl, "CL", NORMAL, "-"), (lim.xbar_lcl, "LCL", ALARM, "--")):
        _hline(a, val, lab, col, ls)
    a.set_title(f"Biểu đồ X̄ theo phút (n = {lim.n}); điểm đỏ là vi phạm quy tắc Nelson")
    a.set_ylabel("°C")
    b.plot(x, sg["s"], color=NORMAL, lw=0.4)
    out_s = ((sg["s"] > lim.s_ucl) | (sg["s"] < lim.s_lcl)).to_numpy()
    b.scatter(x[out_s], sg["s"].to_numpy()[out_s], s=6, color=ALARM, zorder=3)
    for val, lab, col, ls in ((lim.s_ucl, "UCL", ALARM, "--"), (lim.s_cl, "CL", NORMAL, "-"), (lim.s_lcl, "LCL", ALARM, "--")):
        _hline(b, val, lab, col, ls)
    b.set_title("Biểu đồ S theo phút")
    b.set_xlabel("Thứ tự nhóm con (theo thời gian)")
    for ax in (a, b):
        _style(ax)
        _phase_line(ax, p2_start)
    return _save(fig, out / "fig03_xbar_s_theo_phut.png")


def fig_imr(bm: pd.DataFrame, flags: pd.DataFrame, imr, p2_start: int | None, out: Path) -> Path:
    fig, (a, b) = plt.subplots(2, 1, figsize=(12, 7), sharex=True)
    x = np.arange(len(bm))
    v = bm["mean"].to_numpy()
    bad = flags["any_violation"].to_numpy()
    a.plot(x, v, color=NORMAL, lw=1, marker="o", ms=3)
    a.scatter(x[bad], v[bad], s=22, color=ALARM, zorder=3)
    for val, lab, col, ls in ((imr.x_ucl, "UCL", ALARM, "--"), (imr.x_cl, "CL", NORMAL, "-"), (imr.x_lcl, "LCL", ALARM, "--")):
        _hline(a, val, lab, col, ls)
    a.set_title("Biểu đồ I (trung bình từng lô); điểm đỏ là vi phạm quy tắc Nelson")
    a.set_ylabel("°C")
    mr = np.r_[np.nan, np.abs(np.diff(v))]
    b.plot(x, mr, color=NORMAL, lw=1, marker="o", ms=3)
    over = mr > imr.mr_ucl
    b.scatter(x[over], mr[over], s=22, color=ALARM, zorder=3)
    _hline(b, imr.mr_ucl, "UCL", ALARM, "--")
    _hline(b, imr.mr_cl, "CL", NORMAL, "-")
    b.set_title("Biểu đồ MR (khoảng trượt giữa hai lô liên tiếp)")
    b.set_xlabel("Thứ tự lô (theo thời gian)")
    for ax in (a, b):
        _style(ax)
        _phase_line(ax, p2_start)
    return _save(fig, out / "fig04_imr_theo_lo.png")


def fig_cusum(cu: pd.DataFrame, h: float, p2_start: int | None, out: Path) -> Path:
    fig, ax = plt.subplots(figsize=(12, 4))
    x = np.arange(len(cu))
    ax.plot(x, cu["c_plus"], color=WARN, lw=1.2)
    ax.plot(x, -cu["c_minus"], color=NORMAL, lw=1.2)  # nhánh dưới vẽ ngược dấu cho đối xứng với −h
    sig = (cu["signal_up"] | cu["signal_down"]).to_numpy()
    y = np.where(cu["signal_up"], cu["c_plus"], -cu["c_minus"])
    ax.scatter(x[sig], y[sig], s=25, color=ALARM, zorder=3)
    for val in (h, -h):
        ax.axhline(val, color=ALARM, ls="--", lw=1)
        ax.annotate(f"h = {val:+.0f}σ", xy=(1.005, val), xycoords=("axes fraction", "data"), color=ALARM, fontsize=8, va="center")
    ax.set_title("CUSUM dạng bảng trên trung bình từng lô (k = 0,5σ, h = 5σ)")
    ax.set_xlabel("Thứ tự lô")
    ax.set_ylabel("Tổng tích lũy (đơn vị σ)")
    _style(ax)
    _phase_line(ax, p2_start)
    return _save(fig, out / "fig05_cusum.png")


def fig_ewma(ew: pd.DataFrame, bm: pd.DataFrame, mu0: float, p2_start: int | None, out: Path) -> Path:
    fig, ax = plt.subplots(figsize=(12, 4))
    x = np.arange(len(ew))
    ax.plot(x, bm["mean"], color=GREY, lw=0.6, alpha=0.5)
    ax.plot(x, ew["z"], color=NORMAL, lw=1.6)
    ax.plot(x, ew["ucl"], color=ALARM, ls="--", lw=1)
    ax.plot(x, ew["lcl"], color=ALARM, ls="--", lw=1)
    ax.axhline(mu0, color=NORMAL, lw=0.8)
    sig = ew["signal"].to_numpy()
    ax.scatter(x[sig], ew["z"].to_numpy()[sig], s=25, color=ALARM, zorder=3)
    ax.annotate("UCL", xy=(1.005, ew["ucl"].iloc[-1]), xycoords=("axes fraction", "data"), color=ALARM, fontsize=8)
    ax.annotate("LCL", xy=(1.005, ew["lcl"].iloc[-1]), xycoords=("axes fraction", "data"), color=ALARM, fontsize=8)
    ax.set_title("EWMA (λ = 0,2; L = 3) với giới hạn thay đổi theo i; đường xám là trung bình lô")
    ax.set_xlabel("Thứ tự lô")
    ax.set_ylabel("°C")
    _style(ax)
    _phase_line(ax, p2_start)
    return _save(fig, out / "fig06_ewma.png")


def fig_capability(sensor: pd.DataFrame, spec, cap: dict, out: Path, var: str = "uht_temp") -> Path:
    v = sensor[var].dropna().to_numpy(float)
    fig, ax = plt.subplots(figsize=(9, 4.5))
    ax.hist(v, bins=120, density=True, color=NORMAL, alpha=0.8)
    xs = np.linspace(min(v.min(), spec.lsl - 1), max(v.max(), spec.usl + 1), 400)
    ax.plot(xs, stats.norm.pdf(xs, cap["mean"], cap["sigma_overall"]), color=WARN, lw=2)
    for val, lab, col, ls in ((spec.lsl, "LSL", ALARM, "--"), (spec.usl, "USL", ALARM, "--"), (spec.mid, "Target", GREY, "-")):
        ax.axvline(val, color=col, ls=ls, lw=1.5)
        ax.text(val, ax.get_ylim()[1], f" {lab} {val:g}", color=col, fontsize=9, va="top")
    ax.set_title("Năng lực quy trình UHT: phân phối so với giới hạn kỹ thuật")
    ax.set_xlabel("°C")
    ax.text(0.02, 0.62, f"Cp = {cap['Cp']:.2f}   Cpk = {cap['Cpk']:.2f}\nCpm = {cap['Cpm']:.2f}\nPp = {cap['Pp']:.2f}   Ppk = {cap['Ppk']:.2f}\n{cap['conclusion']}",
            transform=ax.transAxes, va="top", fontsize=9, bbox=dict(facecolor="white", edgecolor=GRID))
    _style(ax)
    return _save(fig, out / "fig07_nang_luc_quy_trinh.png")


def fig_corr_heatmap(corr: pd.DataFrame, pval: pd.DataFrame, out: Path, name: str = "Spearman") -> Path:
    k = len(corr)
    fig, ax = plt.subplots(figsize=(max(6, 0.5 * k + 3), max(5, 0.5 * k + 2)))
    im = ax.imshow(corr.values, cmap=DIVERGING, vmin=-1, vmax=1)
    ax.set_xticks(range(k), corr.columns, rotation=60, ha="right", fontsize=7)
    ax.set_yticks(range(k), corr.index, fontsize=7)
    for i in range(k):
        for j in range(k):
            if i != j:
                star = "*" if pval.iloc[i, j] < 0.05 else ""
                ax.text(j, i, f"{corr.iloc[i, j]:.2f}{star}", ha="center", va="center", fontsize=5.5)
    ax.set_title(f"Tương quan {name} giữa các biến theo lô (* là p < 0,05)")
    fig.colorbar(im, ax=ax, shrink=0.7)
    return _save(fig, out / f"fig08_heatmap_tuong_quan_{name.lower()}.png")


def fig_thi(batches: pd.DataFrame, thi_corr: pd.DataFrame, out: Path) -> Path | None:
    from .config import THI_QUALITY_VARS
    qs = [q for q in THI_QUALITY_VARS if q in batches.columns]
    if "thi_index" not in batches.columns or not qs:
        return None
    fig, axes = plt.subplots(2, len(qs), figsize=(4.4 * len(qs), 7), squeeze=False)
    for j, q in enumerate(qs):
        m = batches[["thi_index", q]].dropna()
        a = axes[0][j]
        a.scatter(m["thi_index"], m[q], s=14, color=NORMAL, alpha=0.6)
        if len(m) > 2 and m["thi_index"].nunique() > 1:
            k, c = np.polyfit(m["thi_index"], m[q], 1)
            xs = np.array([m["thi_index"].min(), m["thi_index"].max()])
            a.plot(xs, k * xs + c, color=WARN, lw=2)
        a.axvline(72, color=ALARM, ls="--", lw=1)  # ngưỡng stress nhiệt
        a.text(72, a.get_ylim()[1], " THI = 72", color=ALARM, fontsize=8, va="top")
        row = thi_corr[thi_corr["variable"] == q]
        if len(row):
            r = row.iloc[0]
            a.text(0.03, 0.04, f"Spearman ρ = {r['spearman_rho']:.2f} (p = {r['spearman_p']:.3f})", transform=a.transAxes, fontsize=8)
        a.set_xlabel("thi_index")
        a.set_ylabel(q)
        a.set_title(f"THI và {q}")
        _style(a)
        b = axes[1][j]
        if "is_heat_stress" in batches.columns:
            data = [batches.loc[batches["is_heat_stress"] == g, q].dropna() for g in (0, 1)]
            b.boxplot(data, tick_labels=["Bình thường (0)", "Stress nhiệt (1)"], patch_artist=True, widths=0.5,
                      boxprops=dict(facecolor=NORMAL, alpha=0.35, edgecolor=NORMAL), medianprops=dict(color=WARN, lw=2))
        b.set_title(f"{q} theo is_heat_stress")
        _style(b)
    return _save(fig, out / "fig09_thi_va_chat_luong_sua.png")
