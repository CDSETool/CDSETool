"""
This module provides functions for processing data concurrently
"""

from concurrent.futures import wait, FIRST_COMPLETED
from concurrent.futures import ThreadPoolExecutor


def _concurrent_process(fun, iterable, workers=4):
    """
    Process items in an iterable concurrently

    Items are taken from the iterable as soon as a worker becomes available

    Returns an iterable of the results
    """

    # Futures are submitted in batches instead of all at once to avoid
    # requesting too many items from the iterable at once, which is important if
    # the iterable is a generator that is producing items on the fly.
    #
    # The 1.5 factor is a small overhead to keep jobs > workers at all times, instead
    # of jobs == workers, which could cause the workers to be idle while waiting for
    # the iterable to produce more items.
    low_water_mark = int(workers * 1.5)
    iterator = iter(iterable)

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = []

        # Pluck an item from the iterable and submit it to the executor.
        # If the iterable is exhausted, this function is a no-op.
        def submit_item():
            item = next(iterator, None)
            if item is not None:
                futures.append(executor.submit(fun, item))

        # Fill the futures list up to the low water mark
        def fill_futures():
            for _ in range(low_water_mark - len(futures)):
                submit_item()

        # Submit the first batch of items
        fill_futures()

        # Continue until no more futures are queued
        while futures:

            # Wait for the first future(s) to complete
            done, futures = wait(futures, return_when=FIRST_COMPLETED)
            futures = list(futures)

            for future in done:
                yield future.result()

            # Submit items to replace the ones that are done.
            fill_futures()
