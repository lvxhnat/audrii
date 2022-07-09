import os
import uuid
import time
import itertools
import numpy as np
import pandas as pd
from itertools import cycle
from functools import partial
from typing import Any, Dict, List, Callable, Union
from concurrent.futures import ThreadPoolExecutor, as_completed

from audrii.utilities.logger import logging


class RateBypassWorker:

    def __init__(
        self,
        worker_id: int,
        callback: Callable,
        api_keys: List[str],
        sleep_time: int = 60,
        save_results: bool = False,
        save_result_fn: Callable = None,
        save_result_path: str = None,
    ):
        """ Create a single thread worker that contains independently rotating API Keys and parameter values.

        Parameters
        ---------------
        worker_id: The id tag for the worker. Pass index when iterating over to create the workers.
        callback: The function we will like to create a worker for.
        api_keys: The list of API Keys that the worker will iterate over when rate limit is hit.
        sleep_time: The time a worker should sleep once the rate limit is hit.
        save_results: Whether we want to save the output result for each dataframe.
        save_result_fn [Conditional on save_results]: The function we use for saving our dataframe. It takes in Params(dataframe, save_path)
        save_result_path [Conditional on save_results]: The base path we use on top for saving our data to.

        """
        self.api_keys = cycle(api_keys)
        self.api_key = next(self.api_keys)
        self.num_keys = len(api_keys)
        self.worker_id = worker_id

        self.sleep_time = sleep_time  # The time it takes before the API rate limit is hit
        self.timeout = time.time()

        self.callback_function = callback

        if save_results:
            assert save_result_fn is not None and save_result_path is not None

        self.save_results = save_results
        self.save_results_fn = save_result_fn
        self.save_results_path = save_result_path

        self.rotations = 0
        self.successful_extractions = 0

    def execute(self, constant_params: Union[Dict[str, Any], None], iterable_params: List[Dict[str, Any]]):
        # Assert that our iterable parameters are all the same length
        if constant_params is None or not isinstance(constant_params, dict):
            raise ValueError(
                f"Only constant_params of type Dict[str, Any] is allowed. You entered object of type {type(constant_params)}.")
        if not isinstance(iterable_params, list):
            raise ValueError(
                f"Only iterable_params of type List[Dict[str, Any]] is allowed. You entered object of type {type(iterable_params)}.")
        if constant_params is not None:
            return [self.scrape(**constant_params, **item) for item in iterable_params]
        else:
            return [self.scrape(**item) for item in iterable_params]

    def scrape(self, **kwargs):

        try:
            data = self.callback_function(self.api_key, **kwargs)
            self.successful_extractions += 1
            logging.info(
                f"Worker {self.worker_id}: Extracted {str(self.successful_extractions)} items. On API Key {self.api_key}")
            if self.save_results and data is not None:
                self.save_results_fn(
                    data, self.save_results_path.strip("/") + str(uuid.uuid4()))
            else:
                return data

        except Exception as e:
            self.rotations += 1
            self.api_key = next(self.api_keys)
            # If all the keys have been fully rotated in one revol
            if self.rotations % self.num_keys == 0:
                time_to_sleep = time.time() - self.timeout
                self.timeout = time.time()
                logging.error(
                    f"Worker {self.worker_id}: {str(e)}. Sleep for {time_to_sleep}s")
                time.sleep(time_to_sleep)
            self.scrape(**kwargs)


class DataProducer:

    def __init__(
        self,
        callback: Callable,
        api_keys: List[str],
        rate_limit: str = "5/min",
        save_results: bool = False,
        save_result_fn: Callable = None,
        save_result_path: str = None,
    ):
        self.api_keys = api_keys
        self.callback_function = callback

        self.to_save_results = save_results
        self.save_results_fn = save_result_fn
        self.save_results_path = save_result_path

        if rate_limit.split("/")[1] == "min":
            self.sleep_time = rate_limit.split("/")[1] * 60

    def save_results(self, dataframe: pd.DataFrame, path: str):
        try:
            dataframe.to_parquet(path)
        except Exception as e:
            logging.error(e)

    @staticmethod
    def split_array_for_workers(array: List[str], num: int):
        k, m = divmod(len(array), num)
        d = (array[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(num))
        return d

    def calculate_number_of_workers(self, param_length: int):
        # Max 30 workers or number of api keys available. Minimum 1 worker
        min_api_keys_per_worker = 5
        return min(12, np.ceil(param_length/min_api_keys_per_worker).astype(int))

    def execute(
        self,
        iterables_params: Dict[str, Any],
        constants_params: Dict[str, Any],
    ):
        clients = []
        length = len(list(iterables_params.values())[0])
        num_workers = self.calculate_number_of_workers(length)
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
            worker_id = 0

            for worker_api_key_list, worker_parameter_list in zip(worker_api_keys, worker_parameters):

                worker_id += 1

                fn = RateBypassWorker(
                    worker_id=worker_id,
                    callback=self.callback_function,
                    api_keys=worker_api_key_list,
                    sleep_time=self.sleep_time,
                    save_results=self.to_save_results,
                    save_result_fn=self.save_results_fn,
                    save_result_path=self.save_results_path,
                )
                worker_job = partial(
                    fn.execute, constant_params=constants_params)

                job = executor.submit(
                    worker_job,
                    iterable_params=worker_parameter_list,
                )

                futures.append(job)

                logging.info(
                    f"Worker {worker_id} initialised with {str(len(worker_api_key_list))} keys.")

            for client_num, future in enumerate(as_completed(futures), start=1):
                logging.info(f"Client number {client_num} initialized.")
                client = future.result()
                clients.append(client)

        return list(itertools.chain.from_iterable(clients))
