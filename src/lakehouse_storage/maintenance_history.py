"""Append-only operation evidence; unfinished actions have unknown outcomes."""
import json
import os
from pathlib import Path


class MaintenanceHistory:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def append(self, record):
        with self.path.open('a') as handle:
            handle.write(json.dumps(record, allow_nan=False) + '\n')
            handle.flush()
            os.fsync(handle.fileno())

    def records(self):
        runs = {}
        if self.path.exists():
            for line in self.path.read_text().splitlines():
                record = json.loads(line)
                runs[record['run_id']] = record
        return [dict(record, decision='UNKNOWN', reason='unfinished operation')
                if record['decision'] == 'STARTED' else record for record in runs.values()]

    def latest_action_time(self, table_uri, action):
        times = [r['started_epoch'] for r in self.records()
                 if r['table'] == table_uri and r['action'] == action and r['decision'] in {'RUN', 'UNKNOWN', 'FAIL'}]
        return max(times) if times else None
