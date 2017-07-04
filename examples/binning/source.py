"""Provides a wrapper to randomise whether underlying prices are generated"""

from random import Random as _Random


class Source(object):
    """\
    Provides iterable class wrapping a price source and randomly produces a
    price or not.
    """

    def __init__(self, prob, prices, seed=123):
        self.prob = prob
        self.prices = prices
        self._rand = _Random(seed)

    def __next__(self):
        return self.maybe_next_price()

    def __iter__(self):
        return self

    def maybe_next_price(self):
        """Based on the probability, return a price or None"""

        if self._rand.uniform(0.0, 1.0) <= self.prob:
            return next(self.prices)
        return None
