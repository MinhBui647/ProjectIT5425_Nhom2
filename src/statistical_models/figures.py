"""Hình cho báo cáo. Màu theo quy chuẩn nhóm: #184C78 (bình thường), #D97706 (cảnh báo), #D9534F (lỗi).
Mỗi chuỗi còn được phân biệt bằng kiểu đường/marker và legend, không chỉ bằng màu."""

from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
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

def fig_yield_residuals(res, out: Path) -> Path:
    """Phần dư OLS (lô độc lập): phần dư theo giá trị dự báo và Q–Q."""
    fig, ax = plt.subplots(1, 2, figsize=(9, 3.6))
    r, f = np.asarray(res.resid), np.asarray(res.fittedvalues)
    ax[0].scatter(f, r, s=22, color=BLUE, edgecolor="white", linewidth=0.8)
    ax[0].axhline(0, color=GRAY, lw=1, ls="--")
    ax[0].set(xlabel="Giá trị dự báo (%)", ylabel="Phần dư (%)", title="Phần dư theo giá trị dự báo")
    (osm, osr), (slope, icpt, _) = stats.probplot(r, dist="norm")
    ax[1].scatter(osm, osr, s=22, color=BLUE, edgecolor="white", linewidth=0.8)
    ax[1].plot(osm, slope * np.asarray(osm) + icpt, color=AMBER, lw=1.5, ls="--", label="đường chuẩn")
    ax[1].set(xlabel="Phân vị lý thuyết", ylabel="Phân vị phần dư", title="Q–Q phần dư")
    ax[1].legend(loc="upper left")
    fig.suptitle(f"Bài 1 – OLS {int(res.nobs)} lô độc lập: chẩn đoán phần dư", fontsize=11)
    return _save(fig, out, "fig01_yield_residuals.png")

def fig_enet_path(alphas, coefs, features, alpha_star, l1_ratio, out: Path) -> Path:
    fig, ax = plt.subplots(figsize=(8, 4.2))
    la = np.log10(alphas)
    order = np.argsort(-np.max(np.abs(coefs), axis=1))
    styles = ["-", "--", ":", "-."]
    for rank, j in enumerate(order):
        top = rank < 4
        ax.plot(la, coefs[j], lw=2 if top else 1, color=[BLUE, AMBER, RED, BLUE][rank] if top else "#C7CBD1",
                ls=styles[rank] if top else "-", label=features[j] if top else None, zorder=3 if top else 2)
    ax.axvline(np.log10(alpha_star), color=GRAY, lw=1, ls="--")
    ax.text(np.log10(alpha_star), ax.get_ylim()[1], "alpha chọn bởi GroupKFold ", va="top", ha="right",
            fontsize=8.5, color="#374151")
    ax.axhline(0, color="#9CA3AF", lw=0.8)
    ax.set(xlabel="log10(alpha)", ylabel="Hệ số (biến đã chuẩn hóa)",
           title=f"Bài 1 – Đường hệ số ElasticNet (l1_ratio = {l1_ratio:g}); 4 biến lớn nhất được tô màu")
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.14), ncol=4)
    return _save(fig, out, "fig02_elasticnet_path.png")

def fig_yield_cv(y, oof, label: str, out: Path) -> Path:
    fig, ax = plt.subplots(figsize=(4.8, 4.4))
    lo, hi = min(y.min(), oof.min()), max(y.max(), oof.max())
    ax.plot([lo, hi], [lo, hi], color=GRAY, lw=1, ls="--", label="dự báo hoàn hảo")
    ax.axhline(y.mean(), color=AMBER, lw=1.5, ls=":", label="trung bình y")
    ax.scatter(y, oof, s=22, color=BLUE, edgecolor="white", linewidth=0.8, label="dự báo ngoài mẫu")
    ax.set(xlabel="yield_recovery_pct thật (%)", ylabel="Dự báo ngoài mẫu (%)", title=f"Bài 1 – {label}")
    ax.legend(loc="upper left")
    return _save(fig, out, "fig03_yield_cv_pred_vs_actual.png")

def fig_roc(rocs: dict, out: Path) -> Path:
    fig, axes = plt.subplots(1, len(rocs), figsize=(4.6 * len(rocs), 4.3), squeeze=False)
    for ax, (name, r) in zip(axes[0], rocs.items()):
        ax.plot([0, 1], [0, 1], color="#9CA3AF", lw=1, ls="--")
        for key, col, ls, lab in (("in_sample", BLUE, "-", "trong mẫu"), ("oof", AMBER, "--", "ngoài mẫu (CV)")):
            rr = r[key]
            ax.plot(rr["fpr"], rr["tpr"], color=col, ls=ls, drawstyle="steps-post",
                    label=f"{lab}: AUC = {rr['auc']:.3f}")
        best = r["oof"]
        ax.scatter([1 - best["specificity"]], [best["sensitivity"]], s=60, color=RED, zorder=4, edgecolor="white",
                   label=f"Youden J (CV), ngưỡng = {best['threshold']:.3f}")
        ax.set(xlabel="1 − độ đặc hiệu", ylabel="Độ nhạy", title=f"Bài 2 – ROC ({name})", xlim=(0, 1), ylim=(0, 1.02))
        ax.legend(loc="lower right", fontsize=8.5)
    return _save(fig, out, "fig04_defect_roc.png")

def fig_hosmer(table, hl: dict, name: str, out: Path) -> Path:
    fig, ax = plt.subplots(figsize=(5.4, 4))
    x = np.arange(len(table))
    ax.bar(x - 0.2, table["observed_1"], width=0.38, color=BLUE, label="số lô lỗi quan sát")
    ax.bar(x + 0.2, table["expected_1"], width=0.38, color=AMBER, label="số lô lỗi kỳ vọng")
    ax.set(xticks=x, xticklabels=[f"{i + 1}" for i in x], xlabel="Nhóm theo xác suất dự báo (thấp → cao)",
           ylabel="Số lô lỗi", title=f"Bài 2 – Hosmer–Lemeshow ({name}): p = {hl['p_value']:.3f}")
    ax.legend(loc="upper left")
    return _save(fig, out, "fig05_defect_hosmer_lemeshow.png")

def fig_em_loglik(history, out: Path) -> Path:
    fig, ax = plt.subplots(figsize=(5.6, 3.6))
    ax.plot(np.arange(len(history)), history, color=BLUE, marker="o", ms=4)
    ax.set(xlabel="Vòng lặp EM", ylabel="Log-likelihood dữ liệu quan sát",
           title="Bài 3 – Log-likelihood qua các vòng lặp (không giảm)")
    return _save(fig, out, "fig06_em_loglik.png")

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
    fig.suptitle("Bài 3 – RMSE trên các ô bị xóa; µ và Σ chỉ học trên tập train", fontsize=11)
    fig.tight_layout(rect=(0, 0.08, 1, 1))
    fig.legend(handles, labels, loc="lower center", ncol=3, frameon=False)
    p = out / "fig07_em_rmse.png"
    fig.savefig(p, bbox_inches="tight")
    plt.close(fig)
    
    return p