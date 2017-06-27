"""Provides simple class to compute (reproducible) random prices"""


from random import Random as _Random
from math import fabs as _fabs
from collections import namedtuple as _nt

_STATE = _nt('_STATE', ['last_price', 'random', 'iter'])


class RandomPrices(object):
    """\
    Provides iterable class producing random non-negative and non-zero
    prices.

    Given an initial price p, the next price will be calculated as
    abs(p + p*Gaussian(0.0, sigma)).  The price is additionally floored
    at 0.1.

    This is not intended as a particularly realistic model of a price
    series, but rather as a reproducible source of suitable test data
    for handling price series.  The reproducibility is handled by the seed
    and using a Pseudo RNG (Mersenne twister as provided by Python's random
    module).
    """

    def __init__(self, seed=42, initial_price=100.0, sigma=0.01,
                 max_iter=10_000_000):
        """
        :param seed:  Random number seed
        :param initial_price:  The first price from which to start the price
                               evolution.
        :param sigma:  The sigma of the Gaussian used to generated the price
                       movements.
        :param max_iter:  The total amout of prices to emit before stopping
                          an iteration of this object.
        """
        super().__init__()
        self._seed = seed
        self._initial_price = initial_price
        self._sigma = sigma
        self._max_iter = max_iter
        self._state = None
        self.reset()

    def __iter__(self):
        return self

    def __next__(self):
        if self._state.iter >= self._max_iter:
            raise StopIteration
        return self.next_price()

    def next_price(self):
        """Calculate and return a new price; use initial price on first call"""
        if self._state.iter == 0:
            # On first iteration use initial price
            price = self._initial_price
        else:
            change = self._state.random.gauss(0.0, self._sigma)
            price = _fabs(
                self._state.last_price + self._state.last_price * change
            )
            if price < 0.1:
                price = 0.1
        self._state = _STATE(price, self._state.random, self._state.iter + 1)
        return self._state.last_price

    def reset(self):
        """Reset this object back to the initial state"""
        self._state = _STATE(self._initial_price, _Random(self._seed), 0)
