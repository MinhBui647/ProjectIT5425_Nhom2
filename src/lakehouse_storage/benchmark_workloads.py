"""Equivalent workloads and warm-cache timing with raw repetition evidence."""
import hashlib
import statistics
import time

import polars as pl
from polars.testing import assert_frame_equal

from lakehouse_storage.config import get_storage_options

QUERY_TEXT = {
    'line_date_aggregate': 'scan Delta(version); group by line_id,ingestion_date; count, mean(uht_temp)',
    'batch_lookup': 'scan Delta(version); filter batch_id == bound id; project timestamp,uht_temp; sort timestamp',
    'hourly_aggregate': 'scan Delta(version); group by timestamp.truncate(1h); count, mean(power_kw)',
}
BUSINESS_COLUMNS = ['timestamp', 'line_id', 'preheat_temp', 'uht_temp', 'homo_press_stage1',
                    'homo_press_stage2', 'flow_rate', 'conductivity', 'power_kw']


def query(table_uri, version, name, batch_id):
    frame = pl.scan_delta(table_uri, version=version, storage_options=get_storage_options())
    if name == 'line_date_aggregate':
        result = frame.group_by('line_id', 'ingestion_date').agg(pl.len().alias('rows'), pl.col('uht_temp').mean()).sort('line_id', 'ingestion_date')
    elif name == 'batch_lookup':
        result = frame.filter(pl.col('batch_id') == batch_id).select('timestamp', 'uht_temp').sort('timestamp')
    elif name == 'hourly_aggregate':
        result = frame.group_by(pl.col('timestamp').dt.truncate('1h').alias('hour')).agg(pl.len().alias('rows'), pl.col('power_kw').mean()).sort('hour')
    else:
        raise ValueError('unknown workload')
    return result.collect()


def verify_equal(expected, actual):
    assert_frame_equal(expected, actual, check_exact=False, rel_tol=1e-6, abs_tol=1e-6)


def business_frame(table_uri):
    return pl.scan_delta(table_uri, storage_options=get_storage_options()).select(BUSINESS_COLUMNS).sort('line_id', 'timestamp').collect()


def fingerprint(frame):
    return hashlib.sha256(frame.write_ipc(None).getvalue()).hexdigest()


def measure(callable_, *, repeats=5, warmups=1):
    if repeats < 5 or warmups < 1:
        raise ValueError('benchmark requires at least one warmup and five repetitions')
    for _ in range(warmups):
        callable_()
    samples = []
    for _ in range(repeats):
        start = time.perf_counter()
        callable_()
        samples.append(time.perf_counter() - start)
    quartiles = statistics.quantiles(samples, n=4, method='inclusive')
    return {'raw_seconds': samples, 'median_seconds': statistics.median(samples),
            'iqr_seconds': quartiles[2] - quartiles[0], 'std_seconds': statistics.pstdev(samples)}
