"""A returned failure is a failure even without an exception."""
import pytest
from lakehouse_storage.governance_adapter import classify_result


@pytest.mark.parametrize(('result', 'expected'), [
    ({'status': 'PASS', 'rows_written': 3}, 'DONE'),
    ({'status': 'PASS_WITH_REJECTIONS', 'rows_written': 3}, 'DONE'),
    ({'status': 'SKIPPED_DUPLICATE', 'rows_written': 0}, 'DONE'),
    ({'status': 'FAIL', 'error_category': 'quality', 'rows_written': 0}, 'REJECTED'),
    ({'status': 'FAIL', 'error_category': 'infrastructure', 'rows_written': 0}, 'FAILED'),
])
def test_classification(result, expected):
    assert classify_result(result) == expected


@pytest.mark.parametrize('result', [{}, {'status': 'NEW'}, {'status': 'PASS', 'rows_written': -1}, {'status': 'PASS', 'rows_written': True}])
def test_unknown_result_fails_closed(result):
    with pytest.raises(ValueError): classify_result(result)
