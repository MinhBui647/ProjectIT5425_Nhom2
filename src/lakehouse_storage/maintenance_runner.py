"""One-shot maintenance with metadata-only decisions and durable history."""
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from deltalake import DeltaTable

from lakehouse_storage.config import get_storage_options
from lakehouse_storage.maintenance import compact_table, get_file_sizes, vacuum_table, zorder_table
from lakehouse_storage.maintenance_history import MaintenanceHistory
from lakehouse_storage.service_config import require_local_tree, require_namespace


def snapshot(table_uri):
    table = DeltaTable(table_uri, storage_options=get_storage_options())
    sizes = [f['size_bytes'] for f in get_file_sizes(table_uri)]
    physical = None
    if not table_uri.startswith('s3://'):
        physical = sum(p.stat().st_size for p in Path(table_uri).rglob('*.parquet'))
    return {'version': table.version(), 'file_count': len(sizes), 'file_sizes_bytes': sizes,
            'active_bytes': sum(sizes), 'retained_physical_bytes': physical}


class MaintenanceRunner:
    def __init__(self, history_path):
        self.history = MaintenanceHistory(history_path)
        self.lock = threading.Lock()

    def run(self, policy, action='compact', *, enabled=False, ingestion_stopped=False, now=None):
        now = now or datetime.now(timezone.utc)
        now = now.astimezone(timezone.utc)
        record = {'run_id': uuid.uuid4().hex, 'table': policy.table_uri, 'action': action,
                  'started_at': now.isoformat(), 'started_epoch': now.timestamp(),
                  'decision': 'STARTED', 'reason': '', 'before': None, 'after': None, 'error': None}
        start = time.perf_counter()
        acquired = self.lock.acquire(blocking=False)
        try:
            reason = None
            if not acquired:
                reason = 'another maintenance job is running'
            elif not enabled or not ingestion_stopped:
                reason = 'explicit enabled/stopped-ingestion phase required'
            elif not policy.window_start_hour <= now.hour < policy.window_end_hour:
                reason = 'outside UTC maintenance window'
            last = self.history.latest_action_time(policy.table_uri, action)
            if not reason and last is not None and now.timestamp() - last < policy.cooldown_seconds:
                reason = 'cooldown'
            if reason:
                record.update(decision='SKIP', reason=reason)
                return record
            if action not in {'compact', 'zorder', 'vacuum_preview'}:
                raise ValueError('unsupported maintenance action')
            require_namespace(policy.table_uri, policy.allow_root)
            if not policy.table_uri.startswith('s3://'):
                require_local_tree(policy.table_uri, policy.allow_root)
            record['before'] = snapshot(policy.table_uri)
            if action == 'compact':
                sizes = record['before']['file_sizes_bytes']
                small = sum(size < policy.target_file_size_mb * 1024**2 for size in sizes)
                if len(sizes) < policy.file_count_threshold or not sizes or small / len(sizes) < policy.small_file_ratio:
                    record.update(decision='SKIP', reason='file thresholds not met')
                    return record
            if action == 'zorder':
                table = DeltaTable(policy.table_uri, storage_options=get_storage_options())
                columns = set(table.schema().to_arrow().names)
                partitions = set(table.metadata().partition_columns)
                if not policy.zorder_columns or not set(policy.zorder_columns) <= columns - partitions:
                    raise ValueError('Z-order requires existing non-partition columns')
            self.history.append(record)
            if action == 'compact':
                result = compact_table(policy.table_uri, policy.target_file_size_mb)
            elif action == 'zorder':
                result = zorder_table(policy.table_uri, list(policy.zorder_columns))
            else:
                result = vacuum_table(policy.table_uri, retention_hours=policy.retention_hours,
                                      dry_run=True, enforce_retention_duration=True)
            record.update(decision='RUN', reason='policy accepted', result=result, after=snapshot(policy.table_uri))
            return record
        except Exception as exc:
            record.update(decision='FAIL', reason='operation failed', error=str(exc))
            return record
        finally:
            record.update(ended_at=datetime.now(timezone.utc).isoformat(), duration_seconds=time.perf_counter() - start)
            self.history.append(record)
            if acquired:
                self.lock.release()
