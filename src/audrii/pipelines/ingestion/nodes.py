import os
import numpy as np
import pandas as pd
from typing import List
from pathlib import Path
import dask.dataframe as dd

from audrii.utilities.scraper.api_wrapper import DataProducer
from audrii.utilities.scraper.tickers.polygon_io import get_historical_data
from audrii.utilities.main import get_api_keys
from audrii.utilities.logger import logging


def get_russell_3000_tickers(df: pd.DataFrame) -> List[str]:
    return df.Ticker.to_list()


def ingest_russell_3000(tickers: List[str], skip: bool, params):

    if not skip:

        api_keys = get_api_keys('POLYGONIO_API_KEY')
        producer = DataProducer(get_historical_data, api_keys)

        try:
            data = producer.execute(
                constants_params=params,
                iterables_params={
                    "ticker": tickers,
                })
            df = dd.concat([d for d in data if d is not None])
            return df

        except Exception as e:
            logging.error(f"Error encountered while extracting data. {str(e)}")
            return df

    else:

        return pd.DataFrame()
