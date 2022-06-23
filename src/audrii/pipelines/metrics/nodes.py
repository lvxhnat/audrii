"""
This is a boilerplate pipeline 'data_processing'
generated using Kedro 0.18.1
"""
import os
import warnings
import pandas as pd
from typing import List, Dict
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from audrii.pipelines.metrics.data_ingestion import request_authentication_token, load_historical_stock_data

warnings.filterwarnings("ignore", category=DeprecationWarning)


def scrape_or_ingest_data(args) -> Dict[str, pd.DataFrame]:
    """
    Example Response
    ----------
     >>>      close      high     low     open                 date     volume symbol
            0   177.57  179.2300  177.26  178.085  2021-12-31 00:00:00   64062261   AAPL
            1   182.01  182.8800  177.71  177.830  2022-01-03 00:00:00  104701220   AAPL
            2   179.70  182.9400  179.12  182.630  2022-01-04 00:00:00   99310438   AAPL
            ...
            500 20.87   24.63     20.10   20.63    2022-01-11 00:00:00    99310445   GPS
    """
    ticker_list, resolution, from_date = args['tickers'], args['resolution'], args['from_date']

    token = request_authentication_token()

    def standard_ticker_request(ticker: str):
        return load_historical_stock_data(
            ticker=ticker,
            api_token=token,
            resolution=resolution,
            environment="prod",
            from_date=from_date
        )

    data = {}

    with ThreadPoolExecutor(max_workers=min(len(ticker_list), os.cpu_count())) as executor:

        future_promise = {executor.submit(
            standard_ticker_request, ticker): ticker for ticker in ticker_list}

        for future in as_completed(future_promise):
            response = future.result()
            data[future_promise[future]] = response

    return data


def merge_ingested_data(
        params,
        dfs: Dict[str, pd.DataFrame]):

    tickers = params['tickers']

    datasets = {}
    for columnar in ['close', 'high', 'open', 'low', 'volume']:
        dataset = pd.concat([
            (dfs[ticker][[columnar, "date"]].set_index("date")
             .rename(columns={columnar: ticker})) for ticker in tickers], axis=1).reset_index()
        dataset['datetime'] = dataset.date.apply(lambda x: pd.to_datetime(x, unit="s"))

        assert not dataset.isnull().values.any()

        datasets[columnar] = dataset

    return datasets
