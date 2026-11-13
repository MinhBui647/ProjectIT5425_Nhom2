"""Cấu hình chung cho phân hệ 5 (TV5): giới hạn kỹ thuật, tham số SPC, danh sách cột cần loại."""

from dataclasses import dataclass


@dataclass(frozen=True)
class Spec:
    """Giới hạn kỹ thuật (USL/LSL) của một biến quy trình.

    Lưu ý: đây là giới hạn công nghệ (spec của khách hàng), không phải khoảng hợp lệ
    135–145 trong hợp đồng dữ liệu Great Expectations của TV3. Khoảng của TV3 chỉ để lọc
    dữ liệu rác nên khá rộng, nếu lấy nó tính Cp/Cpk thì sẽ ra con số đẹp (~2,1) nhưng
    không có ý nghĩa thực tế.
    """

    name: str
    lsl: float
    usl: float
    target: float | None = None
    unit: str = ""

    @property
    def mid(self) -> float:
        # không khai báo mục tiêu thì lấy trung điểm của khoảng spec
        return self.target if self.target is not None else (self.lsl + self.usl) / 2


# Theo tài liệu nhiệm vụ TV5: LSL = 137 °C, USL = 142 °C, Target = 140 °C.
SPECS: dict[str, Spec] = {
    "uht_temp": Spec("uht_temp", lsl=137.0, usl=142.0, target=140.0, unit="°C"),
}
CAPABLE_CPK = 1.33  # ngưỡng đạt theo tài liệu

# --- Tham số SPC ---
YEAR = 2025             # chỉ dùng dữ liệu năm 2025 có batch_key
SUBGROUP_SIZE = 60      # nhóm con 1 phút: 60 mẫu 1 Hz (theo tài liệu)
SUBGROUP_EVERY_S = 60   # mỗi phút một nhóm con
PHASE1_MONTHS = (1,)    # Phase I: tháng 01/2025 để ước lượng giới hạn
PHASE2_MONTHS = (2, 3)  # Phase II: tháng 02–03/2025 để giám sát
# hằng số biểu đồ I-MR (cửa sổ 2 điểm)
D2_N2 = 1.128
D4_N2 = 3.267
CUSUM_K = 0.5           # độ lệch tham chiếu k, đơn vị sigma
CUSUM_H = 5.0           # ngưỡng quyết định h, đơn vị sigma
EWMA_LAMBDA = 0.2
EWMA_L = 3.0

# --- Cột không được đưa vào mô hình ---
# nhãn mục tiêu (TV6 dự báo) và các cột khóa/định danh, đưa vào chỉ gây nhiễu hoặc rò rỉ
TARGETS = ("yield_recovery_pct", "batch_defect_flag")
THI_QUALITY_VARS = ("fat_in", "protein_in", "yield_recovery_pct")  # biến chất lượng đối chiếu với THI
GROUP_CANDIDATES = {  # tên cột nhóm có thể có trong batch_features (lấy cột nào có)
    "sản phẩm": ("sku_id", "product_name", "product_key"),
    "ca làm việc": ("shift_code", "shift"),
    "trang trại": ("farm_id", "farm_name", "farm_key"),
}
NON_FEATURE_COLUMNS = (
    "batch_key", "date_key", "start_time_key", "line_key", "product_key", "farm_key",
    "batch_id", "line_id", "sku_id", "farm_id", "full_date", "shift_code",
)
VIF_THRESHOLD = 5.0  # VIF lớn hơn mức này thì coi là đa cộng tuyến, loại dần
