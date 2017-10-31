import find_political_donors

import pytest


@pytest.mark.parametrize("unrounded, rounded_", [
    (1.5, 2),
    (1.49, 1),
    (1.4999999, 1),
    (2.5, 3),
])
def test_round_amount(unrounded, rounded_):
    assert find_political_donors.round_amount(unrounded) == rounded_
