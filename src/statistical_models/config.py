"""Cấu hình chung: biến của từng bài toán, tham số thuật toán."""

YEAR = 2025          # phạm vi: lô năm 2025 có n_readings > 0
SEED = 5425

DEDUP_KEYS = ("fat_in", "protein_in", "acidity_sh", "yield_recovery_pct", "batch_defect_flag")
GROUP_COL = "group_id"

# --- Bài 1: tỷ lệ thu hồi ---
YIELD_TARGET = "yield_recovery_pct"
YIELD_FEATURES = (
    "protein_in", "acidity_sh",                                       # chất lượng sữa (mes_lims)
    "uht_temp_std", "uht_temp_min", "uht_temp_max", "flow_rate_avg",  # thống kê cảm biến theo lô
    "n_anomalies", "homo_press1_avg",
    "day_of_week",                                                    # dim_date
    "humidity_avg_pct", "thi_index", "is_heat_stress",               # thời tiết
    "gdt_smp_usd",                                                    # giá thị trường
)
EN_L1_RATIOS = (0.1, 0.3, 0.5, 0.7, 0.9, 0.95, 1.0)
EN_N_ALPHAS = 100
CV_SPLITS = 5
CV_REPEATS = 5       # lặp kiểm định chéo với 5 cách chia (seed) khác nhau, báo cáo trung bình ± độ lệch chuẩn

# --- Bài 2: lô lỗi ---
DEFECT_TARGET = "batch_defect_flag"
DEFECT_FEATURES = ("uht_temp_min", "flow_rate_avg")
DEFECT_CV_SPLITS = 3
HL_GROUPS = 10

# --- Bài 3: EM cho dữ liệu cảm biến bị khuyết ---
SENSOR_VARS = ("uht_temp", "preheat_temp", "homo_press_stage1", "homo_press_stage2",
               "flow_rate", "conductivity", "power_kw")
EM_SAMPLE_ROWS = 3000
EM_MISSING_RATE = 0.10
EM_MAX_ITER = 500
EM_TOL = 1e-8
EM_OUTAGES_PER_BATCH = 2              # kịch bản mất tín hiệu: mỗi lô valid có 2 cảm biến mất liền một đoạn
EM_OUTAGE_MIN, EM_OUTAGE_MAX = 10, 15  # độ dài đoạn mất tín hiệu (phút), theo kế hoạch TV6
EM_VALID_FRACTION = 0.2   # 20 % số lô làm tập valid (chia theo batch_key, các phút của một lô không bị tách)
