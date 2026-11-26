"""Cấu hình chung: biến của từng bài toán, tham số thuật toán."""

YEAR = 2025          # phạm vi: lô năm 2025 có n_readings > 0
SEED = 5425

# --- Bài 1: giám sát dây chuyền / lô cần kiểm tra (Hotelling T²) ---
DEFECT_TARGET = "batch_defect_flag"  # chỉ để đối chiếu, Phase I bỏ các lô lỗi
T2_ALPHA = 0.0027          # cùng tỉ lệ báo động giả với giới hạn 3 sigma
T2_PHASE1_MONTHS = (1,)    # Phase I (train): tháng 01, giống TV5
T2_BATCH_MAX_FLAG_RATE = 0.05  # chọn k để ≤ 5 % lô Phase I bị gắn cờ
T2_FAULT_SHIFT_SD = 2.0    # lỗi giả lập: lệch 2 sd
T2_FAULT_MINUTES = 15      # trong 15 phút liền
# --- Bài 2: điền dữ liệu cảm biến bị mất (EM) ---
EM_HOLD_SENSOR = "uht_temp"   # điểm kiểm soát tới hạn: không xuất lô bằng số ước lượng
EM_HOLD_MAX_MINUTES = 5       # mất UHT quá số phút này thì giữ lô, kiểm vi sinh
SENSOR_VARS = ("uht_temp", "preheat_temp", "homo_press_stage1", "homo_press_stage2",
               "flow_rate", "conductivity", "power_kw")
EM_SAMPLE_ROWS = 3000
EM_MISSING_RATE = 0.10
EM_MAX_ITER = 500
EM_TOL = 1e-8
EM_OUTAGES_PER_BATCH = 2              # kịch bản mất tín hiệu: mỗi lô valid có 2 cảm biến mất liền một đoạn
EM_OUTAGE_MIN, EM_OUTAGE_MAX = 10, 15  # độ dài đoạn mất tín hiệu (phút), theo kế hoạch TV6
EM_VALID_FRACTION = 0.2   # 20 % số lô làm tập valid (chia theo batch_key, các phút của một lô không bị tách)
