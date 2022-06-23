import os
import csv
import requests
import numpy as np
import pandas as pd

from dotenv import load_dotenv
from datetime import datetime
from typing import Union, List

from audrii.utilities.scraper.interfaces.ticker.alpha_vantage import AssetHistoricalData

env_loaded = load_dotenv()


def get_historical_data(
        api_key: str,
        ticker: str,
        resolution: str = "5min",
        from_date: str = "2022-02-20",
        data_format: str = "json",) -> Union[pd.DataFrame, List[AssetHistoricalData]]:
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

    def get_date_range(from_date: str):
        days = max(13 * 28, (datetime.today() -
                             datetime.strptime(from_date, "%Y-%m-%d")).days)
        months = np.ceil(days / 28).astype(int)
        return "year" + str(months//12) + "month" + str(months % 12)

    resolution = resolution.strip(" ").lower()

    if "-" in from_date:
        date_range = get_date_range(from_date)
    else:
        date_range = from_date

    base_endpoint = "https://www.alphavantage.co/query?"
    endpoint = f"function=TIME_SERIES_INTRADAY_EXTENDED&symbol={ticker}&interval={resolution}&slice={date_range}&apikey={api_key}"

    url = base_endpoint + endpoint

    with requests.Session() as s:
        download = s.get(url)
        decoded_content = download.content.decode('utf-8')
        cr = csv.reader(decoded_content.splitlines(), delimiter=',')
        my_list = list(cr)

    df = pd.DataFrame(my_list[1:], columns=my_list[0]).rename(
        columns={"time": "date"})
    df['symbol'] = ticker

    assert df.shape[0] > 0, "Empty dataframe"

    if data_format == "json":
        return eval(df.to_json(orient="table", index=False))['data']

    elif data_format == "csv":
        return df
