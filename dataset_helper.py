import threading
import itertools
import math

from typing import Iterable
from collections.abc import Callable

def run_threads(queries: Iterable[any],
                coroutine: Callable[None], # type: ignore
                num_threads: int=2 ) -> None:
    
    if num_threads <= 1 or not queries:

        raise Exception("""ERROR: Invalid number of threads or invalid dataset. 
                        Ensure you are using more than one thread and that the dataset is a not None.""")

    num_batches = int( math.floor(len(queries) / num_threads) )
    batches = itertools.batched(queries, n=num_batches)

    threads = list()

    for batch in batches:
        # Spawn thread for coroutine, assigning current data batch as arg
        thread = threading.Thread(
            target=coroutine, 
            args=[batch])
        threads.append(thread)

    # Call into each thread
    for thread in threads: 
        thread.start()

    # Join each thread
    for thread in threads: 
        thread.join() 