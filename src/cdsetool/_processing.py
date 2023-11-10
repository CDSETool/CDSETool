from concurrent.futures import wait, FIRST_COMPLETED 
from concurrent.futures import ThreadPoolExecutor
from cdsetool.query import query_features
import time
import os

def _concurrent_process(fun, iterable, workers=4):
    low_water_mark = int(workers * 1.5)

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = list()

        def submit_item():
            futures.append(executor.submit(fun, next(iterable)))

        for _ in range(low_water_mark):
            submit_item()

        while futures:
            done, futures = wait(futures, return_when=FIRST_COMPLETED)
            futures = list(futures)
            for future in done:
                yield future.result()

            if len(futures) < low_water_mark:
                submit_item()
