"""One-shot maintenance with explicit phase approval and a held local lock."""
import argparse
import json
import os
import sys
from lakehouse_storage.maintenance_policy import MaintenancePolicy
from lakehouse_storage.maintenance_runner import MaintenanceRunner
from lakehouse_storage.service_config import ServiceConfig, require_local_tree
from lakehouse_storage.worker_state import single_instance


def run_once(config, action='compact'):
    if action not in {'compact', 'zorder', 'vacuum_preview'}:
        raise ValueError('unsupported maintenance action')
    tables = os.getenv('MAINTENANCE_TABLES', 'sensor_telemetry,mes_lims').split(',')
    if any(t not in {'sensor_telemetry', 'mes_lims', 'market_prices', 'market_indices', 'weather', 'food_recalls'} for t in tables):
        raise ValueError('unsupported maintenance table')
    with single_instance(config.root / 'demo-phase.lock'):
        require_local_tree(config.root / 'reports', config.root)
        runner = MaintenanceRunner(config.root / 'maintenance.jsonl')
        results = []
        for table in tables:
            policy = MaintenancePolicy(config.silver_root.rstrip('/') + '/' + table, config.silver_allow_root,
                file_count_threshold=int(os.getenv('MAINTENANCE_FILE_THRESHOLD', '50')),
                target_file_size_mb=int(os.getenv('MAINTENANCE_TARGET_MB', '128')),
                cooldown_seconds=float(os.getenv('MAINTENANCE_COOLDOWN_SECONDS', '3600')),
                zorder_columns=tuple(filter(None, os.getenv('MAINTENANCE_ZORDER_COLUMNS', '').split(','))),
                window_start_hour=int(os.getenv('MAINTENANCE_WINDOW_START_UTC', '0')),
                window_end_hour=int(os.getenv('MAINTENANCE_WINDOW_END_UTC', '24')))
            results.append(runner.run(policy, action,
                enabled=os.getenv('MAINTENANCE_ENABLED', 'false').lower() == 'true',
                ingestion_stopped=os.getenv('MAINTENANCE_INGESTION_STOPPED', 'false').lower() == 'true'))
        return results


def main():
    parser = argparse.ArgumentParser(description='One-shot demo maintenance; exclusive phase required; vacuum preview only')
    parser.add_argument('--once', action='store_true')
    parser.add_argument('--action', choices=['compact', 'zorder', 'vacuum_preview'], default='compact')
    parser.add_argument('--stdin', action='store_true')
    args = parser.parse_args()
    try:
        action = json.load(sys.stdin).get('action', 'compact') if args.stdin else args.action
        results = run_once(ServiceConfig.from_env(), action)
        failed = any(r['decision'] == 'FAIL' for r in results)
        print(json.dumps({'status': 'FAILED' if failed else 'SUCCESS', 'results': results}))
        return 3 if failed else 0
    except ValueError as exc:
        print(json.dumps({'status': 'FAILED', 'category': 'business', 'error': str(exc)}))
        return 2
    except Exception as exc:
        print(json.dumps({'status': 'FAILED', 'category': 'infrastructure', 'error': str(exc)}))
        return 3


if __name__ == '__main__':
    raise SystemExit(main())
