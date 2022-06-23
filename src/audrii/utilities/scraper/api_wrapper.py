import os
import time
import itertools
from itertools import cycle
from functools import partial
from typing import Any, Dict, List, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed

from audrii.utilities.logger import logging
from audrii.utilities.scraper.tickers.alpha_vantage import get_historical_data


class RateBypassWorker:

    def __init__(
        self,
        callback: Callable,
        api_keys: List[str],
        sleep_time: int = 1,
        rate_limit: float = None,
    ):
        self.api_keys = cycle(api_keys)
        self.api_key = next(self.api_keys)

        self.sleep_time = sleep_time
        self.rate_limit = rate_limit
        self.callback_function = callback

    def execute(self, constant_params: Dict[str, Any], iterable_params: List[Dict[str, Any]]):
        # Assert that our iterable parameters are all the same length

        return [self.scrape(**constant_params, **item) for item in iterable_params]

    def scrape(self, **kwargs):

        try:
            return self.callback_function(self.api_key, **kwargs)

        except Exception as e:
            logging.error(
                f"Rate limit reached on API Key. Rotating to the next available key.")
            self.api_key = next(self.api_keys)
            self.scrape(**kwargs)

            time.sleep(self.sleep_time)


class DataProducer:

    def __init__(
        self,
        callback: Callable,
        api_keys: List[str],
        rate_limit: str = "5/min",
    ):
        self.api_keys = api_keys
        self.callback_function = callback

        if rate_limit.split("/")[1] == "min":
            self.sleep_time = int(1/int(rate_limit.split("/")[0]))

        self.num_workers = min(
            max(os.cpu_count() + 4, int(len(self.api_keys)/5)), len(self.api_keys))

    @staticmethod
    def split_array_for_workers(array: List[str], num: int):
        k, m = divmod(len(array), num)
        d = (array[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(num))
        return d

    def execute(
        self,
        iterables_params: Dict[str, Any],
        constants_params: Dict[str, Any],
    ):
        clients = []
        length = len(list(iterables_params.values())[0])
        num_workers = min(self.num_workers, length)
        logging.info(f"Generating {str(num_workers)} workers...")

        # Reformat the parameters to feed into the functions

        assert sum(map(lambda x: (len(x) == length), [
                   *iterables_params.values()])) == len(iterables_params.values()), "Ensure that input parameters are of the same length"
        parameter_set = []
        for i in range(length):
            d = {}
            for key, value in iterables_params.items():
                d[key] = value[i]
            parameter_set.append(d)

        # Create the split parameters for each worker
        worker_parameters = DataProducer.split_array_for_workers(
            parameter_set, num_workers)
        worker_api_keys = DataProducer.split_array_for_workers(
            self.api_keys, num_workers)

        with ThreadPoolExecutor(max_workers=num_workers) as executor:

            futures = []

            for worker_api_key_list, worker_parameter_list in zip(worker_api_keys, worker_parameters):

                fn = RateBypassWorker(
                    callback=self.callback_function,
                    api_keys=worker_api_key_list,
                    sleep_time=self.sleep_time,
                )
                worker_job = partial(fn.execute, constant_params=constants_params)

                job = executor.submit(
                    worker_job,
                    iterable_params=worker_parameter_list,
                )

                futures.append(job)

            for client_num, future in enumerate(as_completed(futures), start=1):
                logging.info(f"Client number {client_num} initialized.")
                client = future.result()
                clients.append(client)

        return list(itertools.chain.from_iterable(clients))


if __name__ == '__main__':
    api_keys = [os.environ[f'ALPHA_VANTAGE_API_KEY_{i}'] for i in range(30)]
    producer = DataProducer(get_historical_data, api_keys)
    data = producer.execute(
        constants_params={},
        iterables_params={
            "ticker": ["GPS", "AAPL"],
        })
    print(data)
