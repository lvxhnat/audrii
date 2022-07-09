import csv
import requests
import numpy as np
import pandas as pd

from dotenv import load_dotenv
from datetime import datetime
from typing import Union, List
from pydantic import BaseModel

env_loaded = load_dotenv()


class RateLimitException(Exception):
    def __str__(self):
        return "API call frequency limit hit."


class AssetHistoricalData(BaseModel):
    # Data Returned directly from the scrapers
    close: float
    high: float
    open: float
    low: float
    date: int
    volume: int
    symbol: str


def get_historical_data(
        api_key: str,
        ticker: str,
        resolution: int = 5,
        from_date: str = "2022-02-20",
        data_format: str = "json",
        header=None,
        proxy=None) -> Union[pd.DataFrame, List[AssetHistoricalData]]:
    """ Retrieve historical data from alpha vantage up to the last two years.

    Parameters
    =============
    ticker      : Ticker symbol.
    resolution  : Data interval 1min, 5min, 15min, 30min, 60min
    date_range  : Date in %Y-%m-%d

    Rate Limits
    =============
    5 API Calls / Minute
    500 API Calls / Day

    Example Usage
    =============
    >>> trading_client.get_historical_data( ticker="AAPL", from_date="2022-01-01", data_format = "csv")
    >>> date	            open	        high	        low	            close	        volume  symbol
    0	2021-11-08 20:00:00	121.381548423	121.381548423	121.381548423	121.381548423	130     AAPL
    1	2021-11-08 19:59:00	121.381645887	121.381645887	121.381645887	121.381645887	150     AAPL
    2	2021-11-08 19:45:00	121.381645887	121.381645887	121.381645887	121.381645887	553     AAPL
    3	2021-11-08 19:31:00	121.342660293	121.342660293	121.342660293	121.342660293	259     AAPL
    4	2021-11-08 19:21:00	121.381645887	121.381645887	121.381645887	121.381645887	100     AAPL

    """

    to_date = datetime.today().strftime("%Y-%m-%d")

    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/{resolution}/minute/{from_date}/{to_date}?adjusted=true&sort=asc&limit=50000&apiKey={api_key}"

    with requests.Session() as s:
        if proxy and header:
            download = s.get(url, proxies=proxy, headers=header).json()
        else:
            download = s.get(url).json()

    df = pd.DataFrame(download['results'])
    df['symbol'] = download['ticker']

    if data_format == "json":
        return eval(df.to_json(orient="table", index=False))['data']

    elif data_format == "csv":
        return df
