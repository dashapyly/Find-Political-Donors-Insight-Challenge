class RollingMedian:
    """Class for calculating the rolling median of a stream of numbers.

    Attributes:
        median: Median of the items that have been added to the class.

    Notes:
        This implementation is not efficient and requires O(n) time to add a new item.
    """

    def __init__(self):
        self._data = []

    def add(self, item: float) -> None:
        """Add an item to the rolling median.

        Args:
            item: Item to add to the rolling median.
        """
        self._data.append(item)
        self._data.sort()

    @property
    def median(self) -> float:
        """Return the median of the items that have been added to the class."""
        if len(self._data) % 2 == 0:
            return (self._data[len(self._data) // 2 - 1] + self._data[len(self._data) // 2]) / 2
        else:
            return self._data[len(self._data) // 2]