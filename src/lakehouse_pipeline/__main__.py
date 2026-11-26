"""JSON stage interface; large payloads never cross the orchestration boundary."""
import argparse
import json
import sys
from lakehouse_storage.service_config import ServiceConfig
from lakehouse_storage.worker_state import NeedsReview
from lakehouse_pipeline.pipeline import prepare_run, execute_stage, run_pipeline, BusinessFailure
from lakehouse_pipeline.source_config import SourceConfig


def main():
    parser = argparse.ArgumentParser(description='Finite source-to-Silver pipeline in an isolated demo namespace')
    parser.add_argument('--stage', choices=['prepare', 'bronze', 'verify', 'govern', 'snapshot', 'finalize', 'run'], default='run')
    parser.add_argument('--identity', default='manual-demo')
    parser.add_argument('--manifest')
    parser.add_argument('--table')
    parser.add_argument('--request', default='{}', help='RunRequest JSON; source selection defaults to legacy S1/S2')
    parser.add_argument('--stdin', action='store_true', help='Read stage arguments as JSON from stdin')
    args = vars(parser.parse_args())
    if args.pop('stdin'):
        args.update(json.load(sys.stdin))
    try:
        config = ServiceConfig.from_env()
        path = args.get('manifest')
        if args['stage'] in {'prepare', 'run'} and not path:
            request = json.loads(args['request']) if isinstance(args['request'], str) else args['request']
            source = SourceConfig(**args['source_config']) if args.get('source_config') else None
            path = prepare_run(config, args['identity'], request, source_config=source)
        if args['stage'] == 'run':
            value = run_pipeline(config, path)
            result = {'manifest_path': str(path), 'status': value['status'], 'silver_snapshot': value['silver_snapshot']}
        elif args['stage'] == 'prepare':
            result = {'manifest_path': str(path), 'status': 'SUCCESS'}
        else:
            result = execute_stage(config, path, args['stage'], args.get('table'))
        print(json.dumps(result))
        return 0
    except (ValueError, NeedsReview) as exc:
        print(json.dumps({'status': 'FAILED', 'category': 'business', 'error': str(exc)}))
        return 2
    except Exception as exc:
        print(json.dumps({'status': 'FAILED', 'category': 'infrastructure', 'error': str(exc)}))
        return 3


if __name__ == '__main__':
    raise SystemExit(main())
