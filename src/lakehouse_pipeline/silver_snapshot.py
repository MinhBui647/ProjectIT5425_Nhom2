"""Capture real Delta versions under the caller's single-host mutation lock."""
import polars as pl
from deltalake import DeltaTable
from lakehouse_storage.config import get_storage_options


def capture_snapshot(records, required_sources, optional_sources=()):
    by_table = {}
    for record in records:
        by_table.setdefault(record['table_name'], []).append(record)
    snapshot = {}
    for table in required_sources:
        batches = by_table.get(table, [])
        if not batches or any(r['status'] != 'DONE' and not (r['status'] == 'NO_DATA' and table in optional_sources) for r in batches):
            raise ValueError(f'required source incomplete: {table}')
        active = [r for r in batches if r['status'] == 'DONE']
        if table == 'market_reports' or not active:
            continue
        uris = {r['silver_uri'] for r in active}
        if len(uris) != 1:
            raise ValueError('Silver target conflict')
        uri = uris.pop()
        delta = DeltaTable(uri, storage_options=get_storage_options())
        version = delta.version()
        frame = pl.read_delta(uri, version=version, storage_options=get_storage_options())
        evidence = []
        for record in active:
            committed = record['p3_result']['committed_evidence']
            rows = frame.filter((pl.col('_bronze_sha256') == record['bronze_sha256'])
                                & pl.col('_bronze_file').is_in(committed['bronze_paths'])
                                & (pl.col('_contract_version') == committed['contract_version']))
            if (rows.height != committed['accepted_rows'] or not rows.height
                    or sorted(rows['_quality_run_id'].unique().to_list()) != committed['quality_run_ids']
                    or not set(rows['_quality_status']) <= {'PASS', 'PASS_WITH_REJECTIONS'}):
                raise ValueError('Silver lineage/count/quality evidence mismatch')
            evidence.append({**committed, 'bronze_path': record['bronze_path'],
                             'rejected_rows': record['p3_result'].get('rejected_rows')})
        snapshot[table] = {'uri': uri, 'version': version, 'total_rows': frame.height, 'batches': evidence}
    return snapshot


def read_snapshot(snapshot, table):
    entry = snapshot[table]
    return pl.read_delta(entry['uri'], version=entry['version'], storage_options=get_storage_options())
