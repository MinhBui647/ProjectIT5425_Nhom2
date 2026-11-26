"""Hình cho báo cáo. Màu theo quy chuẩn nhóm: #184C78 (bình thường), #D97706 (cảnh báo), #D9534F (lỗi).
Mỗi chuỗi còn được phân biệt bằng kiểu đường/marker và legend, không chỉ bằng màu."""

from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import matplotlib.ticker  # noqa: E402,F401
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from scipy import stats  # noqa: E402

BLUE, AMBER, RED, GRAY = "#184C78", "#D97706", "#D9534F", "#6B7280"
plt.rcParams.update({
    "figure.dpi": 110, "savefig.dpi": 140, "font.size": 9.5, "axes.titlesize": 10.5,
    "axes.spines.top": False, "axes.spines.right": False, "axes.grid": True,
    "grid.color": "#E5E7EB", "grid.linewidth": 0.6, "axes.edgecolor": "#9CA3AF",
    "legend.frameon": False, "lines.linewidth": 2,
})

def _save(fig, out: Path, name: str) -> Path:
    p = out / name
    fig.tight_layout()
    fig.savefig(p, bbox_inches="tight")
    plt.close(fig)
    return p

def fig_em_loglik(history, out: Path) -> Path:
    fig, ax = plt.subplots(figsize=(5.6, 3.6))
    ax.plot(np.arange(len(history)), history, color=BLUE, marker="o", ms=4)
    ax.set(xlabel="Vòng lặp EM", ylabel="Log-likelihood dữ liệu quan sát",
           title="Điền dữ liệu cảm biến (EM) – Log-likelihood qua các vòng lặp (không giảm)")
    return _save(fig, out, "fig03_em_loglik.png")

def fig_em_rmse(rmse_z, out: Path) -> Path:
    """RMSE trên ô bị xóa: tập train, tập valid MCAR, tập valid mất tín hiệu 10–15 phút."""
    titles = {"train": "Train, MCAR 10 %", "valid_mcar": "Valid, MCAR 10 %",
              "valid_mat_tin_hieu": "Valid, mất tín hiệu 10–15 phút"}
    n_missing = rmse_z[rmse_z["bien"] == "TOAN_BO"].set_index("tap")["so_o_bi_xoa"]
    splits = [k for k in titles if n_missing.get(k, 0) > 0]
    methods = [("trung_binh", AMBER, None, "điền trung bình (của train)"),
               ("noi_suy_thoi_gian", "#9CA3AF", "///", "nội suy theo thời gian trong lô"),
               ("EM", BLUE, None, "EM (kỳ vọng có điều kiện)")]
    fig, axes = plt.subplots(1, len(splits), figsize=(5.2 * len(splits), 4.0), sharey=True, squeeze=False)
    ymax = np.nanmax(rmse_z[[m for m, *_ in methods if m in rmse_z]].to_numpy(float))
    for ax, split in zip(axes[0], splits):
        t = rmse_z[rmse_z["tap"] == split].set_index("bien")
        tot = t.loc["TOAN_BO"]
        t = t.drop(index="TOAN_BO")
        present = [m for m in methods if m[0] in t and t[m[0]].notna().any()]
        w = 0.8 / len(present)
        x = np.arange(len(t))
        for i, (m, col, hatch, lab) in enumerate(present):
            ax.bar(x - 0.4 + w * (i + 0.5), t[m], width=w * 0.92, color=col, hatch=hatch, edgecolor="white",
                   linewidth=0.6, label=lab)
        ax.set(xticks=x, title=f"{titles[split]}\nEM {tot['EM']:.3f} | trung bình {tot['trung_binh']:.3f}"
               + (f" | nội suy {tot['noi_suy_thoi_gian']:.3f}" if pd.notna(tot.get("noi_suy_thoi_gian")) else ""))
        ax.set_xticklabels(t.index, rotation=30, ha="right")
        ax.set_ylim(0, ymax * 1.15)
    axes[0][0].set_ylabel("RMSE (đơn vị z-score)")
    handles, labels = [], []
    for ax in axes[0]:
        for h, lab in zip(*ax.get_legend_handles_labels()):
            if lab not in labels:
                handles.append(h)
                labels.append(lab)
    fig.suptitle("Điền dữ liệu cảm biến (EM) – RMSE trên các ô bị xóa; µ và Σ chỉ học trên tập train", fontsize=11)
    fig.tight_layout(rect=(0, 0.08, 1, 1))
    fig.legend(handles, labels, loc="lower center", ncol=3, frameon=False)
    p = out / "fig04_em_rmse.png"
    fig.savefig(p, bbox_inches="tight")
    plt.close(fig)
    
    return p

def fig_t2_distribution(t2: np.ndarray, p: int, limit: float, rate: float, alpha: float, out: Path) -> Path:
    """Phân phối T² Phase II so với χ²(p)."""
    t2 = np.asarray(t2, float)
    t2 = t2[np.isfinite(t2)]
    fig, ax = plt.subplots(figsize=(7.2, 3.9))
    hi = max(np.quantile(t2, 0.999), limit * 1.15)
    bins = np.linspace(0, hi, 70)
    ax.hist(np.clip(t2, 0, hi), bins=bins, density=True, color=BLUE, alpha=0.85, edgecolor="white", linewidth=0.5,
            label="T² các phút Phase II (lô chưa thấy)")
    x = np.linspace(0.01, hi, 400)
    ax.plot(x, stats.chi2.pdf(x, p), color=AMBER, ls="--", label=f"χ²({p}) lý thuyết")
    ax.axvline(limit, color=RED, lw=1.5)
    ax.text(limit, ax.get_ylim()[1] * 0.92, f"ngưỡng χ² = {limit:.1f}  \nvượt ngưỡng: {100 * rate:.2f} %  \n"
            f"(kỳ vọng α = {100 * alpha:.2f} %)  ", color="#374151", fontsize=8.5, va="top", ha="right")
    ax.set(xlabel="Hotelling T²", ylabel="Mật độ", title="Giám sát – T² trên dữ liệu kiểm định khớp χ² lý thuyết")
    ax.legend(loc="center right")
    return _save(fig, out, "fig01_t2_phan_phoi.png")

def fig_t2_detection(faults: pd.DataFrame, batch_level: pd.DataFrame, out: Path) -> Path:
    """Tỉ lệ lô bị gắn cờ: lô đạt (báo sai) và lô có lỗi giả lập."""
    methods = [("Hotelling T²", BLUE, None), ("3 sigma từng cảm biến", AMBER, "///")]
    scen = list(dict.fromkeys(faults["kich_ban"]))
    labels = dict(zip(faults["kich_ban"], faults["nhan"]))
    groups = ["Lô đạt, không chèn lỗi\n(báo sai)"] + [labels[k] for k in scen]
    vals = {m: [float(batch_level.set_index("phuong_phap").loc[m, "ti_le_gan_co_lo_dat"])]
            + [float(faults[(faults.kich_ban == k) & (faults.phuong_phap == m)]["ti_le_bat_lo_loi"].iloc[0])
               for k in scen] for m, *_ in methods}
    fig, ax = plt.subplots(figsize=(7.6, 4.0))
    x = np.arange(len(groups))
    w = 0.36
    for i, (m, col, hatch) in enumerate(methods):
        k = int(batch_level.set_index("phuong_phap").loc[m, "k"])
        bars = ax.bar(x + (i - 0.5) * w, vals[m], width=w * 0.94, color=col, hatch=hatch, edgecolor="white",
                      linewidth=0.6, label=f"{m} (gắn cờ khi ≥ {k} phút vượt ngưỡng)")
        for r, v in zip(bars, vals[m]):
            ax.text(r.get_x() + r.get_width() / 2, v + 0.015, f"{100 * v:.0f} %", ha="center", fontsize=8.5,
                    color="#374151")
    ax.set(xticks=x, ylim=(0, 1.12), ylabel="Tỉ lệ lô bị gắn cờ",
           title="Giám sát – lô cần kiểm tra trên Phase II: báo sai thấp, bắt lỗi giả lập (15 phút, 2 sd)")
    ax.set_xticklabels(groups, fontsize=8.5)
    ax.yaxis.set_major_formatter(matplotlib.ticker.PercentFormatter(1.0))
    ax.legend(loc="upper left")
    return _save(fig, out, "fig02_t2_bat_loi.png")