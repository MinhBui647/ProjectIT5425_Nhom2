"""Headless PNG visual artifacts for governance run reports."""

from __future__ import annotations

import math
import re
from pathlib import Path
from typing import Any

import polars as pl
from PIL import Image, ImageDraw, ImageFont

from data_governance.config import FIGURES_PATH


WIDTH = 1200
HEIGHT = 675
PALETTE = {
    "navy": "#17324D",
    "blue": "#2F75B5",
    "green": "#70AD47",
    "amber": "#F4B183",
    "red": "#C00000",
    "light_blue": "#D9EAF7",
    "light_gray": "#E7E6E6",
    "dark_gray": "#595959",
    "white": "#FFFFFF",
}
_SAFE_RUN_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


def _font(size: int, *, bold: bool = False):
    name = "DejaVuSans-Bold.ttf" if bold else "DejaVuSans.ttf"
    try:
        return ImageFont.truetype(name, size=size)
    except OSError:
        return ImageFont.load_default()


def _canvas(title: str) -> tuple[Image.Image, ImageDraw.ImageDraw]:
    image = Image.new("RGB", (WIDTH, HEIGHT), PALETTE["white"])
    draw = ImageDraw.Draw(image)
    draw.rectangle((0, 0, WIDTH, 76), fill=PALETTE["navy"])
    draw.text((36, 20), title, fill=PALETTE["white"], font=_font(30, bold=True))
    return image, draw


def render_missingness_heatmap(df: pl.DataFrame, output_path: str | Path) -> Path:
    """Render row/column missingness with per-column null percentages."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    image, draw = _canvas("Data quality - Missingness heatmap")
    columns = df.columns[:18]
    rows = min(df.height, 80)
    if not columns:
        draw.text((60, 150), "No columns available", fill=PALETTE["dark_gray"], font=_font(24))
    else:
        left, top = 70, 140
        grid_width, grid_height = 1030, 390
        cell_width = max(4, grid_width / len(columns))
        cell_height = max(3, grid_height / max(rows, 1))
        for column_index, column in enumerate(columns):
            values = df.get_column(column).head(rows).to_list()
            for row_index, value in enumerate(values):
                color = PALETTE["red"] if value is None else PALETTE["light_blue"]
                x0 = left + column_index * cell_width
                y0 = top + row_index * cell_height
                draw.rectangle(
                    (x0, y0, x0 + cell_width, y0 + cell_height),
                    fill=color,
                    outline=PALETTE["white"],
                )
            ratio = df.get_column(column).null_count() / max(df.height, 1)
            label = f"{column[:10]}\n{ratio:.1%}"
            draw.multiline_text(
                (left + column_index * cell_width, 545),
                label,
                fill=PALETTE["dark_gray"],
                font=_font(11),
                anchor="ma",
                align="center",
            )
        draw.text((70, 100), "Rows (up to 80)", fill=PALETTE["dark_gray"], font=_font(16))
        draw.rectangle((880, 96, 900, 116), fill=PALETTE["red"])
        draw.text((908, 96), "Missing", fill=PALETTE["dark_gray"], font=_font(14))
        draw.rectangle((990, 96, 1010, 116), fill=PALETTE["light_blue"])
        draw.text((1018, 96), "Present", fill=PALETTE["dark_gray"], font=_font(14))
    image.save(output_path, format="PNG")
    return output_path


def render_sensor_physical_bands(
    sensor_df: pl.DataFrame,
    output_path: str | Path,
    *,
    value_column: str = "uht_temp",
    lower_bound: float = 135.0,
    upper_bound: float = 145.0,
) -> Path:
    """Render Sensor values, accepted physical band, and anomaly markers."""
    required = {"timestamp", value_column, "is_anomaly_flag"}
    missing = sorted(required - set(sensor_df.columns))
    if missing:
        raise ValueError(f"Missing sensor figure columns: {missing}")
    data = sensor_df.sort("timestamp").filter(pl.col(value_column).is_not_null())
    if data.is_empty():
        raise ValueError("Sensor figure requires at least one non-null measurement")

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    image, draw = _canvas("Sensor - Physical band and Hampel/Z-score markers")
    plot = (90, 120, 1120, 575)
    values = [float(value) for value in data[value_column].to_list()]
    y_min = min(min(values), lower_bound) - 3
    y_max = max(max(values), upper_bound) + 3

    def x_of(index: int) -> float:
        return plot[0] + index * (plot[2] - plot[0]) / max(len(values) - 1, 1)

    def y_of(value: float) -> float:
        return plot[3] - (value - y_min) * (plot[3] - plot[1]) / (y_max - y_min)

    draw.rectangle(
        (plot[0], y_of(upper_bound), plot[2], y_of(lower_bound)),
        fill="#E2F0D9",
    )
    draw.line((plot[0], y_of(lower_bound), plot[2], y_of(lower_bound)), fill=PALETTE["green"], width=2)
    draw.line((plot[0], y_of(upper_bound), plot[2], y_of(upper_bound)), fill=PALETTE["green"], width=2)
    points = [(x_of(index), y_of(value)) for index, value in enumerate(values)]
    if len(points) > 1:
        draw.line(points, fill=PALETTE["blue"], width=3)
    anomalies = data["is_anomaly_flag"].fill_null(False).to_list()
    for index, flagged in enumerate(anomalies):
        if flagged:
            x, y = points[index]
            draw.ellipse((x - 7, y - 7, x + 7, y + 7), fill=PALETTE["red"])
    draw.rectangle(plot, outline=PALETTE["dark_gray"], width=2)
    draw.text((90, 592), "Time ->", fill=PALETTE["dark_gray"], font=_font(16))
    draw.text((20, 120), f"{value_column} (C)", fill=PALETTE["dark_gray"], font=_font(15))
    draw.text((920, 90), "Green: 135-145 C", fill=PALETTE["green"], font=_font(15, bold=True))
    draw.text((920, 110), "Red: anomaly", fill=PALETTE["red"], font=_font(15, bold=True))
    image.save(output_path, format="PNG")
    return output_path


def _draw_arrow(draw: ImageDraw.ImageDraw, start, end) -> None:
    draw.line((*start, *end), fill=PALETTE["navy"], width=4)
    angle = math.atan2(end[1] - start[1], end[0] - start[0])
    for offset in (-0.5, 0.5):
        point = (
            end[0] - 16 * math.cos(angle + offset),
            end[1] - 16 * math.sin(angle + offset),
        )
        draw.line((*end, *point), fill=PALETTE["navy"], width=4)


def render_lineage_dag(run_metadata: dict[str, Any], output_path: str | Path) -> Path:
    """Render the run's Bronze → governance → Silver/Data Docs lineage."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    image, draw = _canvas("Run lineage - Bronze to governed outputs")
    nodes = [
        ((55, 245, 270, 385), "BRONZE", str(run_metadata.get("bronze_file", "Bronze input"))),
        ((355, 245, 625, 385), "GOVERNANCE", str(run_metadata.get("run_id", "Transform + validate"))),
        ((720, 130, 1120, 260), "SILVER", str(run_metadata.get("silver_uri", "Silver Delta"))),
        ((720, 390, 1120, 520), "DATA DOCS", str(run_metadata.get("data_docs", "Quality artifacts"))),
    ]
    _draw_arrow(draw, (270, 315), (355, 315))
    _draw_arrow(draw, (625, 290), (720, 205))
    _draw_arrow(draw, (625, 340), (720, 455))
    for index, (box, heading, detail) in enumerate(nodes):
        fill = PALETTE["light_blue"] if index < 2 else "#E2F0D9"
        draw.rounded_rectangle(box, radius=18, fill=fill, outline=PALETTE["navy"], width=3)
        draw.text((box[0] + 18, box[1] + 18), heading, fill=PALETTE["navy"], font=_font(22, bold=True))
        short_detail = detail if len(detail) <= 48 else f"…{detail[-47:]}"
        draw.text((box[0] + 18, box[1] + 62), short_detail, fill=PALETTE["dark_gray"], font=_font(14))
    image.save(output_path, format="PNG")
    return output_path


def generate_required_figures(
    quality_df: pl.DataFrame,
    sensor_df: pl.DataFrame,
    run_metadata: dict[str, Any],
    *,
    run_id: str,
    figures_root: str | Path = FIGURES_PATH,
) -> dict[str, Path]:
    """Generate all three required report figures into one run folder."""
    if not _SAFE_RUN_ID.fullmatch(run_id):
        raise ValueError(f"Unsafe run_id: {run_id!r}")
    output_dir = Path(figures_root) / run_id
    return {
        "missingness_heatmap": render_missingness_heatmap(
            quality_df, output_dir / "missingness_heatmap.png"
        ),
        "sensor_physical_bands": render_sensor_physical_bands(
            sensor_df, output_dir / "sensor_physical_bands.png"
        ),
        "lineage_dag": render_lineage_dag(
            run_metadata, output_dir / "lineage_dag.png"
        ),
    }

