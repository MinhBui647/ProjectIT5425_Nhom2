"""Single-process UTC scheduler; ingestion must stop before enabling mutation."""
import hashlib
import os
import signal
import threading

from lakehouse_storage.maintenance_policy import MaintenancePolicy
from lakehouse_storage.maintenance_runner import MaintenanceRunner
from lakehouse_storage.service_config import ServiceConfig
from lakehouse_storage.worker_state import single_instance


def build_scheduler(policies, runner, *, enabled=False, ingestion_stopped=False, interval_seconds=None):
    from apscheduler.executors.pool import ThreadPoolExecutor
    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.triggers.cron import CronTrigger
    from apscheduler.triggers.interval import IntervalTrigger

    if interval_seconds is not None and not 0.05 <= interval_seconds <= 86400:
        raise ValueError('invalid scheduler interval')
    scheduler = BackgroundScheduler(timezone='UTC', executors={'default': ThreadPoolExecutor(1)})
    for policy in policies:
        identifier = hashlib.sha256(policy.table_uri.encode()).hexdigest()[:16]
        trigger = (IntervalTrigger(seconds=interval_seconds, timezone='UTC') if interval_seconds is not None
                   else CronTrigger.from_crontab(policy.cron_utc, timezone='UTC'))
        scheduler.add_job(runner.run, trigger, args=[policy],
                          kwargs={'enabled': enabled, 'ingestion_stopped': ingestion_stopped},
                          id='maintenance-' + identifier, replace_existing=True,
                          max_instances=1, coalesce=True, misfire_grace_time=30)
    return scheduler


def main():
    import argparse
    import json
    parser = argparse.ArgumentParser(description='Maintenance requires stopped ingestion; vacuum is preview-only.')
    parser.add_argument('--once', action='store_true')
    parser.add_argument('--action', choices=['compact', 'zorder', 'vacuum_preview'], default='compact')
    args = parser.parse_args()
    config = ServiceConfig.from_env()
    tables = os.getenv('MAINTENANCE_TABLES', 'sensor_telemetry,mes_lims').split(',')
    if any(t not in {'sensor_telemetry', 'mes_lims'} for t in tables):
        parser.error('demo maintenance supports sensor_telemetry and mes_lims only')
    policies = [MaintenancePolicy(
        config.silver_root.rstrip('/') + '/' + table, config.silver_allow_root,
        file_count_threshold=int(os.getenv('MAINTENANCE_FILE_THRESHOLD', '50')),
        target_file_size_mb=int(os.getenv('MAINTENANCE_TARGET_MB', '128')),
        cooldown_seconds=float(os.getenv('MAINTENANCE_COOLDOWN_SECONDS', '3600')),
        zorder_columns=tuple(filter(None, os.getenv('MAINTENANCE_ZORDER_COLUMNS', '').split(','))),
        window_start_hour=int(os.getenv('MAINTENANCE_WINDOW_START_UTC', '0')),
        window_end_hour=int(os.getenv('MAINTENANCE_WINDOW_END_UTC', '24')),
        cron_utc=os.getenv('MAINTENANCE_CRON_UTC', '0 * * * *'),
    ) for table in tables]
    enabled = os.getenv('MAINTENANCE_ENABLED', 'false').lower() == 'true'
    stopped = os.getenv('MAINTENANCE_INGESTION_STOPPED', 'false').lower() == 'true'
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    with single_instance(config.root / 'demo-phase.lock'):
        runner = MaintenanceRunner(config.root / 'maintenance.jsonl')
        if args.once:
            results = [runner.run(p, args.action, enabled=enabled, ingestion_stopped=stopped) for p in policies]
            print(json.dumps(results), flush=True)
            return int(any(r['decision'] == 'FAIL' for r in results))
        interval = os.getenv('MAINTENANCE_INTERVAL_SECONDS')
        scheduler = build_scheduler(policies, runner, enabled=enabled, ingestion_stopped=stopped,
                                    interval_seconds=float(interval) if interval else None)
        scheduler.start()
        print('scheduler ready', flush=True)
        try:
            while not stop.wait(0.5):
                pass
        finally:
            scheduler.shutdown(wait=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
