import json
import pytest
from lakehouse_pipeline.run_manifest import ManifestStore


def test_atomic_manifest_binding_corruption_and_sanitized_config(tmp_path):
    store = ManifestStore(tmp_path / 'storage_demo')
    manifest = store.prepare('dag/run-1', {'sources': ['weather'], 'window': ['a', 'b']})
    assert store.prepare('dag/run-1', {'window': ['a', 'b'], 'sources': ['weather']}) == manifest
    with pytest.raises(ValueError):
        store.prepare('dag/run-1', {'sources': ['food_recalls']})
    with pytest.raises(ValueError):
        store.prepare('x', {'AWS_SECRET_ACCESS_KEY': 'do-not-record'})
    path = store.path('dag/run-1')
    path.write_text('{bad')
    with pytest.raises(ValueError):
        store.load(path)


def test_nested_secrets_and_unknown_manifest_versions_rejected(tmp_path):
    store = ManifestStore(tmp_path / 'storage_demo')
    with pytest.raises(ValueError, match='secret'):
        store.prepare('bad', {'request': {'password': 'do-not-record'}})
    store.prepare('x', {'sources': ['weather']})
    path = store.path('x')
    value = json.loads(path.read_text())
    value['schema_version'] = 99
    path.write_text(json.dumps(value))
    with pytest.raises(ValueError, match='invalid'):
        store.load(path)
