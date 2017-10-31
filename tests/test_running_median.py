import pytest

from running_median import RunningMedian

RUNNING_MEDIAN_TEST_DATA = [
    ([], None),
    ([5, 4, 3, 2, 1, 1, 2, 3, 4, 5], 3),
    (range(101), 50),
    (reversed(range(101)), 50),
    (range(100_001), 50_000),
    (reversed(range(100_001)), 50_000),
    (range(1_000_001), 500_000),
]

@pytest.mark.parametrize("cls, data, median_", [
    (cls, data, median_)
    for cls in [RunningMedian]
    for (data, median_) in RUNNING_MEDIAN_TEST_DATA])
def test_running_median(cls, data, median_):
    running_median = cls()
    for item in data:
        running_median.add(item)
    assert running_median.median == median_