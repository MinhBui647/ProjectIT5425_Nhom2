GOLD_TABLES = {
    "dim_date": {
        "columns": [
            ("date_key", "UInt32"),
            ("full_date", "Date"),
            ("day", "UInt8"),
            ("month", "UInt8"),
            ("quarter", "UInt8"),
            ("year", "UInt16"),
            ("day_of_week", "UInt8"),
            ("is_weekend", "UInt8"),
        ],
        "order_by": "date_key",
    },
    "dim_time": {
        "columns": [
            ("time_key", "UInt16"),
            ("hour", "UInt8"),
            ("minute", "UInt8"),
            ("shift_code", "LowCardinality(String)"),
        ],
        "order_by": "time_key",
    },
    "dim_line": {
        "columns": [("line_key", "UInt16"), ("line_id", "String")],
        "order_by": "line_key",
    },
    "dim_product": {
        "columns": [("product_key", "UInt16"), ("sku_id", "String")],
        "order_by": "product_key",
    },
    "dim_batch": {
        "columns": [
            ("batch_key", "UInt32"),
            ("batch_id", "String"),
            ("production_start", "Nullable(DateTime64(6, 'UTC'))"),
            ("production_end", "Nullable(DateTime64(6, 'UTC'))"),
            ("microbiology_status", "Nullable(String)"),
        ],
        "order_by": "batch_key",
    },
    "dim_farm": {
        "columns": [
            ("farm_key", "UInt16"),
            ("farm_id", "String"),
            ("farm_name", "String"),
            ("latitude", "Float32"),
            ("longitude", "Float32"),
            ("valid_from", "Date"),
            ("valid_to", "Nullable(Date)"),
            ("is_current", "UInt8"),
        ],
        "order_by": "farm_key",
    },
    "fact_sensor_telemetry": {
        "columns": [
            ("timestamp", "DateTime64(6, 'UTC')"),
            ("date_key", "UInt32"),
            ("time_key", "UInt16"),
            ("line_key", "UInt16"),
            ("batch_key", "Nullable(UInt32)"),
            ("preheat_temp", "Nullable(Float32)"),
            ("uht_temp", "Float32"),
            ("homo_press_stage1", "Nullable(Float32)"),
            ("homo_press_stage2", "Nullable(Float32)"),
            ("flow_rate", "Nullable(Float32)"),
            ("conductivity", "Nullable(Float32)"),
            ("power_kw", "Nullable(Float32)"),
            ("is_anomaly_flag", "UInt8"),
        ],
        "order_by": "(line_key, timestamp)",
        "partition_by": "toYYYYMM(timestamp)",
    },
    "fact_batch_production": {
        "columns": [
            ("batch_key", "UInt32"),
            ("date_key", "UInt32"),
            ("start_time_key", "UInt16"),
            ("line_key", "UInt16"),
            ("product_key", "UInt16"),
            ("farm_key", "Nullable(UInt16)"),
            ("duration_min", "Nullable(Float32)"),
            ("fat_in", "Nullable(Float32)"),
            ("protein_in", "Nullable(Float32)"),
            ("acidity_sh", "Nullable(Float32)"),
            ("yield_recovery_pct", "Nullable(Float32)"),
            ("batch_defect_flag", "UInt8"),
            ("n_readings", "UInt32"),
            ("uht_temp_avg", "Nullable(Float32)"),
            ("uht_temp_std", "Nullable(Float32)"),
            ("uht_temp_min", "Nullable(Float32)"),
            ("uht_temp_max", "Nullable(Float32)"),
            ("flow_rate_avg", "Nullable(Float32)"),
            ("power_kwh", "Nullable(Float32)"),
            ("n_anomalies", "UInt32"),
        ],
        "order_by": "(line_key, date_key, batch_key)",
    },
    "fact_external_market_weather": {
        "columns": [
            ("date_key", "UInt32"),
            ("farm_key", "UInt16"),
            ("temperature_avg_c", "Nullable(Float32)"),
            ("humidity_avg_pct", "Nullable(Float32)"),
            ("precipitation_mm", "Nullable(Float32)"),
            ("thi_index", "Nullable(Float32)"),
            ("is_heat_stress", "UInt8"),
            ("gdt_wmp_usd", "Nullable(Float32)"),
            ("gdt_smp_usd", "Nullable(Float32)"),
            ("gdt_amf_usd", "Nullable(Float32)"),
            ("fao_dairy_index", "Nullable(Float32)"),
        ],
        "order_by": "(farm_key, date_key)",
    },
}


def column_names(table: str) -> list[str]:
    return [name for name, _ in GOLD_TABLES[table]["columns"]]


def create_table_sql(table: str, database: str = "dairy") -> str:
    spec = GOLD_TABLES[table]
    cols = ",\n    ".join(f"`{name}` {ch_type}" for name, ch_type in spec["columns"])
    partition = f"\nPARTITION BY {spec['partition_by']}" if "partition_by" in spec else ""
    return (
        f"CREATE TABLE IF NOT EXISTS {database}.{table} (\n    {cols}\n)\n"
        f"ENGINE = MergeTree{partition}\n"
        f"ORDER BY {spec['order_by']}\n"
        "SETTINGS index_granularity = 8192"
    )