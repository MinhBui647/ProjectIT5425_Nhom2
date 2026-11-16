"""Lazy bridge to P3; never manufactures governance columns or PASS results."""
from pathlib import Path

from lakehouse_storage.service_config import require_namespace


def classify_result(result):
    status = result.get('status')
    rows = result.get('rows_written')
    if type(rows) is not int or rows < 0:
        raise ValueError('invalid P3 rows_written')
    if status in {'PASS', 'PASS_WITH_REJECTIONS', 'SKIPPED_DUPLICATE'}:
        return 'DONE'
    if status == 'FAIL' and rows == 0:
        return 'FAILED' if result.get('error_category') == 'infrastructure' else 'REJECTED'
    raise ValueError(f'unrecognized P3 outcome: {status}')


def process_file(path, table_name, config):
    from data_governance.config import DEFAULT_ERROR_THRESHOLD
    from data_governance.run_governance import process_bronze_file

    path = Path(require_namespace(path, config.root / 'bronze'))
    require_namespace(config.silver_root, config.silver_allow_root)
    result = process_bronze_file(
        path, bronze_root=config.root / 'bronze', table_name=table_name,
        silver_root=config.silver_root, fail_threshold=DEFAULT_ERROR_THRESHOLD,
        date_from=None, date_to=None, dry_run=False, emitter=None,
        reports_root=config.root / 'reports' / 'quality',
        quarantine_root=config.root / 'quarantine',
        figures_root=config.root / 'reports' / 'figures', gx_context=None,
    )
    classify_result(result)
    if result.get('table_name') != table_name or result.get('bronze_file') != str(path):
        raise ValueError('P3 result identity mismatch')
    return result
