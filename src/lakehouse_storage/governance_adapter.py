"""Lazy bridge to P3; never manufactures governance columns or PASS results."""
from pathlib import Path

from lakehouse_storage.bronze_writer import ensure_within_bronze
from lakehouse_storage.config import get_storage_options
from lakehouse_storage.schemas import normalize_table_name, get_silver_schema
from lakehouse_storage.service_config import require_namespace, require_local_tree


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


def ingest_governed_bronze_file(
    bronze_path: str | Path,
    table_name: str,
    *,
    bronze_root: str | Path,
    silver_root: str,
    reports_root: str | Path,
) -> dict:
    """Run unchanged P3 with explicit roots; return its complete gate result.

    This sequential entry point does not coordinate concurrent writers. Legacy
    tables lacking governance metadata fail without migration. Optional P3
    dependencies load only when this function is called.
    """
    table_name = normalize_table_name(table_name)
    get_silver_schema(table_name)
    root = Path(bronze_root).resolve()
    path = ensure_within_bronze(bronze_path, root)
    silver_root = str(silver_root).rstrip('/\\')
    if not silver_root:
        raise ValueError('silver_root must not be empty')
    table_uri = f'{silver_root}/{table_name}' if silver_root.startswith('s3://') else str((Path(silver_root) / table_name).resolve())
    reports = Path(reports_root).resolve()
    try:
        from deltalake import DeltaTable
        from deltalake.exceptions import TableNotFoundError
        try:
            existing = DeltaTable(table_uri, storage_options=get_storage_options())
        except TableNotFoundError:
            existing = None
        if existing is not None:
            names = existing.schema().to_arrow().names
            missing = [col for col in ('_contract_version', '_quality_status', '_quality_run_id') if col not in names]
            if missing:
                raise ValueError(f'Legacy Silver table missing governance columns {missing}; explicit migration required')

        from data_governance.config import DEFAULT_ERROR_THRESHOLD
        from data_governance.run_governance import process_bronze_file
        result = process_bronze_file(
            path, bronze_root=root, table_name=table_name, silver_root=silver_root,
            fail_threshold=DEFAULT_ERROR_THRESHOLD, date_from=None, date_to=None,
            dry_run=False, emitter=None, reports_root=reports / 'quality',
            quarantine_root=reports / 'quarantine', figures_root=reports / 'figures',
            gx_context=None,
        )
        classify_result(result)
        if result.get('table_name') != table_name or result.get('bronze_file') != str(path):
            raise ValueError('P3 result identity mismatch')
        return result
    except Exception as exc:
        return {
            'status': 'FAIL', 'rows_written': 0, 'error_category': 'infrastructure',
            'error': str(exc), 'table_name': table_name, 'bronze_file': str(path),
            'silver_uri': table_uri,
        }


def process_file(path, table_name, config):
    from data_governance.config import DEFAULT_ERROR_THRESHOLD
    from data_governance.run_governance import process_bronze_file

    path = Path(require_namespace(path, config.root / 'bronze'))
    require_namespace(config.silver_root, config.silver_allow_root)
    if not config.silver_root.startswith('s3://'):
        require_local_tree(Path(config.silver_root) / table_name, config.silver_allow_root)
    for child in ('reports', 'quarantine'):
        require_local_tree(config.root / child, config.root)
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
