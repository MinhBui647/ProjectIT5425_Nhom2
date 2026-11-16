"""Deliberate raw faults; business validation remains P3's responsibility."""
import os
import time


def apply_payload_fault(rows, scenario):
    for row in rows:
        if scenario == 'invalid-value':
            row['uht_temp'] = -10.0
        elif scenario == 'invalid-time':
            row['timestamp'] = 'invalid-time'
        elif scenario == 'missing-field':
            row.pop('line_id', None)
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
