class RunningMedian:
    """Class for calculating the rolling median of a stream of numbers.

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
        i = 0
        inserted = False
        while not inserted and i < len(self._data):
            if self._data[i] >= item:
                self._data.insert(i, item)
                inserted = True
            i += 1
        if not inserted:
            self._data.append(item)

    @property
    def median(self) -> float:
        """Return the median of the items that have been added to the class."""
        if len(self._data) % 2 == 0:
            return (self._data[len(self._data) // 2 - 1] + self._data[len(self._data) // 2]) / 2
        else:
            return self._data[len(self._data) // 2]