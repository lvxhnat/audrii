import os
import numpy as np
import pandas as pd
from typing import List

from audrii.utilities.scraper.api_wrapper import DataProducer
from audrii.utilities.scraper.tickers.polygon_io import get_historical_data


def get_russell_3000_tickers(df: pd.DataFrame) -> List[str]:
    return df.Ticker.to_list()[:15]


def ingest_russell_3000(tickers: List[str], params):

    api_keys = [os.environ[f'POLYGONIO_API_KEY_{i}'] for i in range(2)]
    producer = DataProducer(get_historical_data, api_keys)

    ticker_chunks = DataProducer.split_array_for_workers(
        tickers, np.ceil(len(tickers)/30).astype(int))

    for chunk_index, ticker_chunk in enumerate(ticker_chunks):
        data = producer.execute(
            constants_params=params,
            iterables_params={
                "ticker": ticker_chunk,
            })
        pd.concat(data).to_parquet(
            f"../../../data/01_raw/{chunk_index}_chunk.parquet", index=False)

    return data
