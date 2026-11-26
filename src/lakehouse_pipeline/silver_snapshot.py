"""Capture real Delta versions under the caller's single-host mutation lock."""
import polars as pl
from deltalake import DeltaTable
from lakehouse_storage.config import get_storage_options


def _active_batches(records, required_sources, optional_sources):
    by_table = {}
    for record in records:
        by_table.setdefault(record['table_name'], []).append(record)
    active_by_table = {}
    for table in required_sources:
        batches = by_table.get(table, [])
        if not batches or any(r['status'] != 'DONE' and not (r['status'] == 'NO_DATA' and table in optional_sources) for r in batches):
            raise ValueError(f'required source incomplete: {table}')
        active = [r for r in batches if r['status'] == 'DONE']
        if table == 'market_reports' or not active:
            continue
        if len({r['silver_uri'] for r in active}) != 1:
            raise ValueError('Silver target conflict')
        active_by_table[table] = active
    return active_by_table


def _describe_version(uri, version, active):
    frame = _read_version(uri, version)
    evidence = []
    for record in active:
        committed = record['p3_result']['committed_evidence']
        rows = frame.filter((pl.col('_bronze_sha256') == record['bronze_sha256'])
                            & pl.col('_bronze_file').is_in(committed['bronze_paths'])
                            & (pl.col('_contract_version') == committed['contract_version']))
        statuses = sorted(rows['_quality_status'].unique().to_list())
        if (rows.height != committed['accepted_rows'] or not rows.height
                or sorted(rows['_quality_run_id'].unique().to_list()) != committed['quality_run_ids']
                or statuses != committed['quality_statuses']
                or not set(statuses) <= {'PASS', 'PASS_WITH_REJECTIONS'}):
            raise ValueError('Silver lineage/count/quality evidence mismatch')
        evidence.append({**committed, 'bronze_path': record['bronze_path'],
                         'rejected_rows': record['p3_result'].get('rejected_rows')})
    return {'uri': uri, 'version': version, 'total_rows': frame.height, 'batches': evidence}


def capture_snapshot(records, required_sources, optional_sources=()):
    snapshot = {}
    for table, active in _active_batches(records, required_sources, optional_sources).items():
        uri = active[0]['silver_uri']
        delta = DeltaTable(uri, storage_options=get_storage_options())
        snapshot[table] = _describe_version(uri, delta.version(), active)
    return snapshot


def validate_snapshot(snapshot, records, required_sources, optional_sources=()):
    """Verify durable evidence at its pinned versions, never against latest."""
    active_by_table = _active_batches(records, required_sources, optional_sources)
    if not isinstance(snapshot, dict) or set(snapshot) != set(active_by_table):
        raise ValueError('pinned snapshot tables missing or unexpected')
    for table, active in active_by_table.items():
        entry = snapshot[table]
        if (not isinstance(entry, dict) or entry.get('uri') != active[0]['silver_uri']
                or type(entry.get('version')) is not int or entry['version'] < 0
                or type(entry.get('total_rows')) is not int or entry['total_rows'] < 0):
            raise ValueError(f'invalid pinned snapshot: {table}')
        if entry != _describe_version(entry['uri'], entry['version'], active):
            raise ValueError(f'pinned snapshot evidence mismatch: {table}')


def read_snapshot(snapshot, table):
    entry = snapshot[table]
    return _read_version(entry['uri'], entry['version'])


def _read_version(uri, version):
    delta = DeltaTable(uri, version=version, storage_options=get_storage_options())
    if delta.version() != version:
        raise ValueError('pinned snapshot version unavailable')
    return pl.read_delta(delta, storage_options=get_storage_options())
