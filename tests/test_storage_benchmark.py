"""Benchmark acceptance concerns correctness and evidence, never speed."""
import json
import pytest
from storage_demo_helpers import mock_http


def test_benchmark_evidence_and_result_equality(tmp_path):
    from lakehouse_storage.benchmark import run_benchmark
    root = tmp_path / 'storage_benchmark'
    report = tmp_path / 'report'
    with mock_http(tmp_path / 'storage_demo') as (url, _):
        result = run_benchmark(mock_url=url, work_root=root, silver_root=str(root / 'silver'),
                               allow_root=str(root), report_dir=report, rows=40, batch_sizes=[10,20], repeats=5, warmups=1)
    assert len(result['configurations']) == 2
    fingerprints = set()
    for config in result['configurations']:
        assert config['input_sensor_rows'] == config['accepted_sensor_rows'] == 40
        assert config['correctness_verified']
        fingerprints.add(config['accepted_business_sha256'])
        assert config['before']['file_count'] > config['after']['file_count']
        for layout in ['baseline', 'compacted']:
            for query in config['queries'][layout].values():
                assert len(query['raw_seconds']) == 5
                assert query['median_seconds'] >= 0
                assert query['iqr_seconds'] >= 0
        assert config['throughput_scope'] == 'HTTP + Bronze + P3 validation/reporting + Delta (including MES)'
    assert len(fingerprints) == 1
    assert json.loads((report/'metrics.json').read_text())['environment']['packages']['pyarrow']
    assert (report/'queries.csv').exists() and (report/'operations.jsonl').exists()


@pytest.mark.parametrize('kwargs', [{'repeats':4}, {'warmups':0}, {'rows':0}, {'batch_sizes':[0]}])
def test_invalid_experiment_controls(tmp_path, kwargs):
    from lakehouse_storage.benchmark import run_benchmark
    with pytest.raises(ValueError):
        run_benchmark(mock_url='http://unused', work_root=tmp_path/'storage_benchmark', silver_root=str(tmp_path/'storage_benchmark'/'silver'),
                      allow_root=str(tmp_path/'storage_benchmark'), report_dir=tmp_path/'reports', **kwargs)


def test_query_measurement_and_correctness_guard():
    import polars as pl
    from lakehouse_storage.benchmark_workloads import measure, verify_equal
    count = []
    metric = measure(lambda: count.append(1), repeats=5, warmups=1)
    assert len(count) == 6 and len(metric['raw_seconds']) == 5
    with pytest.raises(AssertionError): verify_equal(pl.DataFrame({'x':[1]}), pl.DataFrame({'x':[2]}))
