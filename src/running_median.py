import heapq


class RunningMedian:
    """Class for calculating the rolling median of a stream of numbers.

    Notes:
        This implementation is supposed to be the most efficient and requires O(n) time to add a new item.
    """

    def __init__(self):
        self._botton_half = []
        self._top_half = []

    def add(self, item: float) -> None:
        """Add an item to the rolling median.

        Args:
            item: Item to add to the rolling median.
        """
        if self.median is None or item <= self.median:
            heapq.heappush(self._botton_half, -item)
        else:
            heapq.heappush(self._top_half, item)
        self._rebalance()

    @property
    def median(self) -> float:
        """Return the median of the items that have been added to the class."""
        if not self._botton_half and not self._top_half:
            return None
        elif len(self._botton_half) > len(self._top_half):
            return -self._botton_half[0]
        elif len(self._top_half) > len(self._botton_half):
            return self._top_half[0]
        else:
            return (-self._botton_half[0] + self._top_half[0]) / 2

    def _rebalance(self):
        while len(self._botton_half) > len(self._top_half) + 1:
            heapq.heappush(self._top_half, -heapq.heappop(self._botton_half))
        while len(self._top_half) > len(self._botton_half) + 1:
            heapq.heappush(self._botton_half, -heapq.heappop(self._top_half))


class RunningMedianList1:
    """Class for calculating the rolling median of a stream of numbers.

    Notes:
        This implementation is not efficient and requires O(n log n) time to add a new item.
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
        if not self._data:
            return None
        elif len(self._data) % 2 == 0:
            return (self._data[len(self._data) // 2 - 1] + self._data[len(self._data) // 2]) / 2
        else:
            return self._data[len(self._data) // 2]


class RunningMedianList2:
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
        if not self._data:
            return None
        elif len(self._data) % 2 == 0:
            return (self._data[len(self._data) // 2 - 1] + self._data[len(self._data) // 2]) / 2
        else:
            return self._data[len(self._data) // 2]
