"""Deliberate raw faults; business validation remains P3's responsibility."""
import os
import time


def apply_payload_fault(rows, scenario, table="sensor_telemetry"):
    fields = {
        'sensor_telemetry': ('uht_temp', 'timestamp', 'line_id'),
        'mes_lims': ('fat_in', 'production_start', 'batch_id'),
        'market_prices': ('AveragePublishedPrice', 'EventDate', 'ProductGroupCode'),
        'market_indices': ('index_value', 'period_start', 'source'),
        'weather': ('relative_humidity_pct', 'observed_at', 'farm_id'),
        'food_recalls': ('classification', 'report_date', 'recall_number'),
        'market_reports': ('section', 'published_date', 'published_date'),
    }
    value, date, required = fields[table]
    for row in rows:
        if table == 'market_indices' and 'Month' in row:
            value, date, required = 'Dairy Price Index', 'Month', 'Dairy Price Index'
        if scenario == 'invalid-value':
            row[value] = 'INVALID' if isinstance(row.get(value), str) else -10
        elif scenario == 'invalid-time':
            row[date] = 'invalid-time'
        elif scenario == 'missing-field':
            row.pop(required, None)
    return rows


def delivery_status(store, run_id, sequence):
    request = store.run(run_id)['request']
    if sequence == 0:
        return 200
    if request['scenario'] == 'delay':
        time.sleep(request['delay_seconds'])
    if request['scenario'] == 'transient-500':
        counter = store._path(run_id) / f'{sequence}.attempts'
        attempt = int(counter.read_text()) if counter.exists() else 0
        temporary = counter.with_suffix('.tmp')
        temporary.write_text(str(attempt + 1))
        os.replace(temporary, counter)
        if attempt < request['failure_attempts']:
            return 500
    return 200
