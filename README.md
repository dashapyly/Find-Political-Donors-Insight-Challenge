# Find Political Donors

## Requirements

- `python >= 3.6`
- `pandas`
- `pytest`

## Unit tests

The unit tests can be found in the `./tests` folder and can be ran using `python -m pytest ./tests`.

## Implementation

Our approach uses the standard Python + Pandas stack for data analysis.
We calculate the running median using two heaps because it scales better to large datasets than approaches which use
a sorted list.