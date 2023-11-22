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
    iterator = iter(iterable)
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = set()

        def submit_item():
            futures.add(executor.submit(fun, next(iterator)))

        for _ in range(workers):
            submit_item()

        while futures:
            done, futures = wait(futures, return_when=FIRST_COMPLETED)
            for future in done:
                yield future.result()

            if len(futures) < workers:
                submit_item()
