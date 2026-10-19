"""Pure Bronze-to-canonical transformations.

Transformers only normalize shape and dtypes. Quality rules, outlier detection,
and gate decisions are deliberately handled by later pipeline stages.
"""

from __future__ import annotations

import polars as pl

from lakehouse_storage.schemas import (
    UNSUPPORTED_SILVER_TABLES,
    get_silver_schema,
    normalize_table_name,
)


SENSOR_MEASUREMENT_COLUMNS = (
    "preheat_temp",
    "uht_temp",
    "homo_press_stage1",
    "homo_press_stage2",
    "flow_rate",
    "conductivity",
    "power_kw",
)

SENSOR_CANONICAL_COLUMNS = (
    "timestamp",
    "line_id",
    "batch_id",
    *SENSOR_MEASUREMENT_COLUMNS,
)

MES_TIMESTAMP_COLUMNS = ("production_start", "production_end")
MES_CHEMISTRY_COLUMNS = (
    "fat_in",
    "protein_in",
    "acidity_sh",
    "yield_recovery_pct",
)
MES_CANONICAL_COLUMNS = (
    "batch_id",
    "line_id",
    *MES_TIMESTAMP_COLUMNS,
    "sku_id",
    *MES_CHEMISTRY_COLUMNS,
    "microbiology_status",
    "batch_defect_flag",
)

WEATHER_REQUIRED_COLUMNS = (
    "farm_id",
    "observed_at",
    "temperature_celsius",
    "relative_humidity_pct",
    "precipitation_mm",
    "_created_at",
)

GDT_REQUIRED_COLUMNS = (
    "EventDate",
    "ProductGroupCode",
    "AveragePublishedPrice",
    "_created_at",
)

FAO_WIDE_INDEX_COLUMNS = (
    "Food Price Index",
    "Meat Price Index",
    "Dairy Price Index",
    "Cereals Price Index",
    "Oils Price Index",
    "Sugar Price Index",
)
FAO_LONG_REQUIRED_COLUMNS = (
    "source",
    "index_name",
    "period_start",
    "index_value",
)

FDA_REQUIRED_COLUMNS = (
    "recall_number",
    "report_date",
    "classification",
    "_created_at",
)


def _require_columns(df: pl.DataFrame, columns: tuple[str, ...], table_name: str) -> None:
    missing = sorted(set(columns) - set(df.columns))
    if missing:
        raise ValueError(f"Missing required columns for {table_name}: {missing}")


def _utc_datetime_expr(df: pl.DataFrame, column: str) -> pl.Expr:
    dtype = df.schema[column]
    expression = pl.col(column)
    if isinstance(dtype, pl.Datetime):
        if dtype.time_zone is None:
            return expression.dt.replace_time_zone("UTC").cast(
                pl.Datetime(time_unit="us", time_zone="UTC")
            )
        return expression.dt.convert_time_zone("UTC").cast(
            pl.Datetime(time_unit="us", time_zone="UTC")
        )
    return expression.cast(pl.String).str.to_datetime(
        time_unit="us",
        time_zone="UTC",
        strict=False,
    )


def _metadata_expressions(df: pl.DataFrame) -> list[pl.Expr]:
    return [pl.col(column) for column in df.columns if column.startswith("_")]


def _date_expr(df: pl.DataFrame, column: str) -> pl.Expr:
    dtype = df.schema[column]
    if dtype == pl.Date:
        return pl.col(column)
    value = pl.col(column).cast(pl.String)
    return value.str.strptime(pl.Date, "%Y-%m-%d", strict=False).fill_null(
        (value + "-01").str.strptime(pl.Date, "%Y-%m-%d", strict=False)
    )


def _optional_string_expr(df: pl.DataFrame, column: str) -> pl.Expr:
    if column in df.columns:
        return pl.col(column).cast(pl.String)
    return pl.lit(None).cast(pl.String)


def _fda_report_datetime_expr() -> pl.Expr:
    value = pl.col("report_date").cast(pl.String)
    compact_date = (
        value.str.strptime(pl.Date, "%Y%m%d", strict=False)
        .cast(pl.Datetime(time_unit="us"))
        .dt.replace_time_zone("UTC")
    )
    iso_datetime = value.str.to_datetime(
        time_unit="us",
        time_zone="UTC",
        strict=False,
    )
    return pl.coalesce(compact_date, iso_datetime)


def transform_sensor_telemetry(df: pl.DataFrame) -> pl.DataFrame:
    """Normalize Sensor Bronze data while preserving all metadata columns."""
    _require_columns(df, SENSOR_CANONICAL_COLUMNS, "sensor_telemetry")
    expressions = [_utc_datetime_expr(df, "timestamp").alias("timestamp")]
    expressions.extend(
        pl.col(column).cast(pl.Float32, strict=False).alias(column)
        for column in SENSOR_MEASUREMENT_COLUMNS
    )
    return df.with_columns(expressions)


def transform_mes_lims(df: pl.DataFrame) -> pl.DataFrame:
    """Normalize MES/LIMS Bronze data while preserving all metadata columns."""
    _require_columns(df, MES_CANONICAL_COLUMNS, "mes_lims")
    expressions = [
        _utc_datetime_expr(df, column).alias(column)
        for column in MES_TIMESTAMP_COLUMNS
    ]
    expressions.extend(
        pl.col(column).cast(pl.Float32, strict=False).alias(column)
        for column in MES_CHEMISTRY_COLUMNS
    )
    expressions.append(
        pl.col("batch_defect_flag")
        .cast(pl.Int8, strict=False)
        .alias("batch_defect_flag")
    )
    return df.with_columns(expressions)


def transform_weather(df: pl.DataFrame) -> pl.DataFrame:
    """Normalize OpenMeteo Bronze data and derive THI heat-stress fields."""
    _require_columns(df, WEATHER_REQUIRED_COLUMNS, "weather")
    normalized = df.rename({"temperature_celsius": "temperature_c"}).with_columns(
        _utc_datetime_expr(df, "observed_at").alias("observed_at"),
        _utc_datetime_expr(df, "_created_at").alias("ingested_at"),
        pl.col("temperature_c").cast(pl.Float32, strict=False),
        pl.col("relative_humidity_pct").cast(pl.Float32, strict=False),
        pl.col("precipitation_mm").cast(pl.Float32, strict=False),
    )
    temperature = pl.col("temperature_c")
    humidity = pl.col("relative_humidity_pct")
    thi = (
        (1.8 * temperature + 32)
        - (0.55 - 0.0055 * humidity) * (1.8 * temperature - 26)
    ).cast(pl.Float32)
    with_thi = normalized.with_columns(thi.alias("thi_index"))
    return with_thi.with_columns(
        pl.when(pl.col("thi_index").is_not_null())
        .then(pl.col("thi_index") > 72.0)
        .otherwise(None)
        .cast(pl.Boolean)
        .alias("is_heat_stress")
    )


def transform_market_prices(df: pl.DataFrame) -> pl.DataFrame:
    """Map raw GDT event prices to the canonical market price contract."""
    _require_columns(df, GDT_REQUIRED_COLUMNS, "market_prices")
    return df.select(
        pl.lit("GDT").alias("source"),
        pl.col("ProductGroupCode").cast(pl.String).alias("product"),
        pl.lit("EVENT_AVERAGE").alias("contract"),
        _utc_datetime_expr(df, "EventDate").alias("observed_at"),
        pl.col("AveragePublishedPrice")
        .cast(pl.Float32, strict=False)
        .alias("price"),
        pl.lit("USD").alias("currency"),
        pl.lit("metric_ton").alias("unit"),
        _utc_datetime_expr(df, "_created_at").alias("ingested_at"),
        *_metadata_expressions(df),
    )


def _transform_fao_long(df: pl.DataFrame) -> pl.DataFrame:
    _require_columns(df, FAO_LONG_REQUIRED_COLUMNS, "market_indices")
    if "ingested_at" in df.columns:
        ingested_at = _utc_datetime_expr(df, "ingested_at")
    elif "_created_at" in df.columns:
        ingested_at = _utc_datetime_expr(df, "_created_at")
    else:
        raise ValueError(
            "Missing required ingestion timestamp for market_indices: "
            "expected ingested_at or _created_at"
        )
    base_period = (
        pl.col("base_period").cast(pl.String)
        if "base_period" in df.columns
        else pl.lit(None).cast(pl.String)
    )
    return df.select(
        pl.lit("FAO").alias("source"),
        pl.col("index_name").cast(pl.String),
        _date_expr(df, "period_start").alias("period_start"),
        pl.col("index_value").cast(pl.Float32, strict=False),
        base_period.alias("base_period"),
        ingested_at.alias("ingested_at"),
        *_metadata_expressions(df),
    )


def _transform_fao_wide(df: pl.DataFrame) -> pl.DataFrame:
    _require_columns(
        df,
        ("Month", *FAO_WIDE_INDEX_COLUMNS, "_created_at"),
        "market_indices",
    )
    metadata_columns = [column for column in df.columns if column.startswith("_")]
    long_frame = df.unpivot(
        on=list(FAO_WIDE_INDEX_COLUMNS),
        index=["Month", *metadata_columns],
        variable_name="index_name",
        value_name="index_value",
    )
    return long_frame.select(
        pl.lit("FAO").alias("source"),
        pl.col("index_name").cast(pl.String),
        _date_expr(long_frame, "Month").alias("period_start"),
        pl.col("index_value").cast(pl.Float32, strict=False),
        pl.lit(None).cast(pl.String).alias("base_period"),
        _utc_datetime_expr(long_frame, "_created_at").alias("ingested_at"),
        *_metadata_expressions(long_frame),
    )


def transform_market_indices(df: pl.DataFrame) -> pl.DataFrame:
    """Normalize either supported FAO long or wide Bronze variant."""
    if set(FAO_LONG_REQUIRED_COLUMNS).issubset(df.columns):
        return _transform_fao_long(df)
    if "Month" in df.columns:
        return _transform_fao_wide(df)
    raise ValueError(
        "Unsupported market_indices Bronze shape: expected FAO long or wide columns"
    )


def transform_food_recalls(df: pl.DataFrame) -> pl.DataFrame:
    """Map raw OpenFDA recall records to the canonical recall contract."""
    _require_columns(df, FDA_REQUIRED_COLUMNS, "food_recalls")
    return df.select(
        pl.lit("FDA").alias("source"),
        pl.col("recall_number").cast(pl.String).alias("recall_id"),
        _fda_report_datetime_expr().alias("published_at"),
        _optional_string_expr(df, "product_description").alias("product"),
        _optional_string_expr(df, "reason_for_recall").alias("reason"),
        _optional_string_expr(df, "status").alias("status"),
        pl.col("classification").cast(pl.String),
        _optional_string_expr(df, "_base_api").alias("source_url"),
        _utc_datetime_expr(df, "_created_at").alias("ingested_at"),
        *_metadata_expressions(df),
    )


_TRANSFORMERS = {
    "sensor_telemetry": transform_sensor_telemetry,
    "mes_lims": transform_mes_lims,
    "weather": transform_weather,
    "market_prices": transform_market_prices,
    "market_indices": transform_market_indices,
    "food_recalls": transform_food_recalls,
}

_BUSINESS_DATE_COLUMNS = {
    "sensor_telemetry": "timestamp",
    "mes_lims": "production_start",
    "weather": "observed_at",
    "market_prices": "observed_at",
    "market_indices": "period_start",
    "food_recalls": "published_at",
}


def _assert_required_silver_columns(df: pl.DataFrame, table_name: str) -> None:
    schema = get_silver_schema(table_name)
    required = {field.name for field in schema if not field.nullable}
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(
            f"Transformer output missing required Silver columns for "
            f"{table_name}: {missing}"
        )


def transform_bronze(df: pl.DataFrame, table_name: str) -> pl.DataFrame:
    """Transform one of the six MVP tables through a stable public API."""
    normalized_name = normalize_table_name(table_name)
    if normalized_name in UNSUPPORTED_SILVER_TABLES:
        raise ValueError(
            f"Unsupported Silver table '{normalized_name}': "
            f"{UNSUPPORTED_SILVER_TABLES[normalized_name]}"
        )
    transformer = _TRANSFORMERS.get(normalized_name)
    if transformer is None:
        raise ValueError(f"Unknown governance table: {normalized_name}")

    transformed = transformer(df)
    _assert_required_silver_columns(transformed, normalized_name)
    business_date_column = _BUSINESS_DATE_COLUMNS[normalized_name]
    return transformed.with_columns(
        pl.col(business_date_column).cast(pl.Date).alias("ingestion_date")
    )

