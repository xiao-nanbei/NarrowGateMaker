"""Bounded exact (start, end] trade-price range queries in event order."""
from bisect import bisect_right
from collections import deque


class TradePriceRangeIndex:
    def __init__(self, block_size=128):
        if block_size not in (64, 128, 256):
            raise ValueError("unsupported range block size")
        self.block_size = block_size
        self.blocks = deque()
        self.cutoff = None

    def append(self, now, price):
        if self.blocks and now < self.blocks[-1][0][-1]:
            raise ValueError("trade range clock moved backwards")
        if not self.blocks or len(self.blocks[-1][0]) == self.block_size:
            self.blocks.append([[], [], price, price])
        block = self.blocks[-1]
        block[0].append(now)
        block[1].append(price)
        block[2] = min(block[2], price)
        block[3] = max(block[3], price)

    def expire(self, cutoff):
        self.cutoff = cutoff
        while self.blocks and self.blocks[0][0][-1] <= cutoff:
            self.blocks.popleft()

    def clear(self):
        self.blocks.clear()
        self.cutoff = None

    def range_minmax(self, start_exclusive, end_inclusive):
        if self.cutoff is not None:
            start_exclusive = max(start_exclusive, self.cutoff)
        if start_exclusive >= end_inclusive:
            return None
        lo = hi = None
        for times, prices, minimum, maximum in self.blocks:
            if times[-1] <= start_exclusive:
                continue
            if times[0] > end_inclusive:
                break
            if start_exclusive < times[0] and times[-1] <= end_inclusive:
                left, right = minimum, maximum
            else:
                a, b = bisect_right(times, start_exclusive), bisect_right(times, end_inclusive)
                if a == b:
                    continue
                selected = prices[a:b]
                left, right = min(selected), max(selected)
            lo = left if lo is None else min(lo, left)
            hi = right if hi is None else max(hi, right)
        return None if lo is None else (lo, hi)
