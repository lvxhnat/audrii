import os
import json
import requests
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta
load_dotenv()

PRODUCTION_VISSER_ENDPOINT = "https://visser-sierra.herokuapp.com"
DEVELOPMENT_VISSER_ENDPOINT = "http://localhost:8080"

VISSER_USER_NAME = os.environ['MONGODB_USER']
VISSER_PASS_WORD = os.environ['MONGODB_PASS']

def request_authentication_token(environment: str = "prod"):
    api_token = requests.post(f"""{PRODUCTION_VISSER_ENDPOINT if environment == "prod" else DEVELOPMENT_VISSER_ENDPOINT}/token""", data = { 
        "username": VISSER_USER_NAME, 
        "password": VISSER_PASS_WORD
    }).json()
    return api_token['access_token']

def request_historical_data(
    api_token: str, 
    ticker: str, 
    from_date: str, 
    to_date: str, 
    environment: str = "prod",
    resolution: str = "15M", 
    instrument: str = "Stock"
): 
    data = requests.post(f"""{PRODUCTION_VISSER_ENDPOINT if environment == "prod" else DEVELOPMENT_VISSER_ENDPOINT}/api/trading/historical""", 
              data = json.dumps({
                  "ticker": ticker,
                  "from_date": from_date,
                  "to_date": to_date,
                  "resolution": resolution,
                  "instrument": "Stock",
              }),
             headers = {
                 "token": api_token
             }).json()
    return data

def get_ticker_historical_data(ticker):
    data = pd.DataFrame(request_historical_data(
                        api_token=request_authentication_token(),
                        ticker=ticker,
                        from_date=(datetime.today() - timedelta(days=365)).strftime("%Y-%m-%d"),
                        to_date=datetime.today().strftime("%Y-%m-%d"),
                        resolution="1D"))
    
    return data
