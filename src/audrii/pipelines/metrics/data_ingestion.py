import os
import json
import requests
import pandas as pd
from typing import List
from dotenv import load_dotenv
from datetime import datetime, timedelta
load_dotenv()

LOCAL_DATA_LOAD_PATH = "/Users/lohyikuang/Downloads/personal_projects/the_heron_project/audrii/data"
BASE_DATA_PATH = LOCAL_DATA_LOAD_PATH + "/01_raw/ticker_data"
PRODUCTION_VISSER_ENDPOINT = "https://visser-sierra.herokuapp.com"
DEVELOPMENT_VISSER_ENDPOINT = "http://localhost:8080"


def request_authentication_token(environment: str = "prod"):
    api_token = requests.post(f"""{PRODUCTION_VISSER_ENDPOINT if environment == "prod" else DEVELOPMENT_VISSER_ENDPOINT}/token""", data={
        "username": os.environ['MONGODB_USER'],
        "password": os.environ['MONGODB_PASS']
    }).json()
    return api_token['access_token']


def load_historical_stock_data(
        ticker: str,
        api_token: str,
        resolution: str = "1D",
        environment: str = "prod",
        instrument: str = "Stock",
        from_date: str = (datetime.today() - timedelta(days=365)).strftime("%Y-%m-%d")) -> pd.DataFrame:

    to_date = datetime.today().strftime("%Y-%m-%d")

    data_save_root = f"{ticker}/{resolution}/{from_date}"
    data_save_path = f"{BASE_DATA_PATH}/{data_save_root}"
    file_name = f"/{to_date}.parquet"

    if not os.path.exists(data_save_path):
        os.makedirs(data_save_path)

    if file_name in os.listdir(data_save_path):
        data = pd.read_parquet(data_save_path + file_name)
        return data

    else:
        data = requests.post(f"""{PRODUCTION_VISSER_ENDPOINT if environment == "prod" else DEVELOPMENT_VISSER_ENDPOINT}/api/trading/historical""",
                             data=json.dumps({
                                 "ticker": ticker,
                                 "from_date": from_date,
                                 "to_date": to_date,
                                 "resolution": resolution,
                                 "instrument": instrument,
                             }),
                             headers={
                                 "token": api_token
                             }).json()

        data = pd.DataFrame(data)
        data.to_parquet(f"{BASE_DATA_PATH}/{data_save_root}/{file_name}")
        return data
