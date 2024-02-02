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
    low_water_mark = int(workers * 1.5)
    iterator = iter(iterable)

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = []

        def submit_item():
            item = next(iterator, None)
            if item is not None:
                futures.append(executor.submit(fun, item))

        for _ in range(low_water_mark):
            submit_item()

        while futures:
            done, futures = wait(futures, return_when=FIRST_COMPLETED)
            futures = list(futures)
            for future in done:
                yield future.result()

            if len(futures) < low_water_mark:
                submit_item()
