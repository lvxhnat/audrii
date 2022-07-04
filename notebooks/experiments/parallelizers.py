from typing import Callable, List, Dict, Any
from concurrent.futures import ThreadPoolExecutor

def parallelize_factory(f: Callable,
                        iterables_params: List[Dict[str, Any]],
                        constants_params: Dict[str, Any] = None):
    
    """ Parallelize a function call with multiple parameters. Great for I/O Bound functions.
    
    Example Usage
    ---------------
    ticker_data = parallelize_factory(cloud_util.read_file_from_gcs, [
        {"prefix": "tickers/historical_ticks_5/AAPL_"},
        {"prefix": "tickers/historical_ticks_5/M_"},
        {"prefix": "tickers/historical_ticks_5/GPS_"},
        {"prefix": "tickers/historical_ticks_5/NFLX_"},
    ])
    
    """
    
    if constants_params is not None:
        worker_job = partial(f, constant_params = constants_params)
    else: 
        worker_job = f
    
    data = []
    
    with ThreadPoolExecutor(min(30, len(iterables_params[0]))) as executor:
        
        running_tasks = [
            executor.submit(worker_job, **iterable_params) for iterable_params in iterables_params]

        for running_task in running_tasks:
            data.append(running_task.result())
    
    return data