"""Report figures map measured metrics without asserting a speedup."""
import json

from lakehouse_storage.benchmark_report import chart_data, render_report


def metrics():
    return {'rows':100, 'seed':42, 'repeats':5, 'cache_policy':'warm', 'environment':{'platform':'test'}, 'configurations':[
        {'batch_size':10, 'input_sensor_rows':100, 'accepted_sensor_rows':100, 'rejected_sensor_rows':0,
         'correctness_verified':True, 'before':{'file_count':10,'file_sizes_bytes':[100]*10,'active_bytes':1000,'retained_physical_bytes':None},
         'after':{'file_count':1,'file_sizes_bytes':[1000],'active_bytes':1000,'retained_physical_bytes':None},
         'input_rows_per_second':250.0, 'pipeline_seconds':0.4, 'compaction_seconds':0.1,
         'queries':{layout:{'batch_lookup':{'median_seconds':0.01,'std_seconds':0.001,'iqr_seconds':0.002,'raw_seconds':[0.01]*5}} for layout in ['baseline','compacted']}}
    ]}


def test_metric_mapping_and_render_smoke(tmp_path):
    value = metrics()
    mapped = chart_data(value)
    assert mapped['batch_sizes'] == [10]
    assert mapped['files_before'] == [10] and mapped['files_after'] == [1]
    assert mapped['throughput'] == [250.0]
    path = tmp_path/'metrics.json'; path.write_text(json.dumps(value))
    paths = render_report(path)
    assert len(paths) == 8
    assert all(p.stat().st_size > 1000 for p in paths)
    assert (tmp_path/'summary.md').exists()
