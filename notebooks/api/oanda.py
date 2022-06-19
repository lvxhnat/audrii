import json
import os
import tsfresh
import requests

"""
REST API
120 requests per second. Excess requests will receive HTTP 429 error. This restriction is applied against the requesting IP address.

Streaming API
20 active streams. Requests above this threshold will be rejected. This restriction is applied against the requesting IP address.
"""
### This needs to be a live account key!
account_bearer_token = "cc1b797ae1c7e97d0fa07a9be34995fd-43194ee3ee242c6efae32997b7ee021b"

granularities_mapping = {
    "S5": timedelta(seconds = 5), 
    "S10": timedelta(seconds = 10), 
    "S15": timedelta(seconds = 15), 
    "S30": timedelta(seconds = 30), 
    "M1": timedelta(minutes = 1), 
    "M2": timedelta(minutes = 2), 
    "M4": timedelta(minutes = 4), 
    "M5": timedelta(minutes = 5), 
    "M10": timedelta(minutes = 10), 
    "M15": timedelta(minutes = 15), 
    "M30": timedelta(minutes = 30), 
    "H1": timedelta(hours = 1), 
    "H2": timedelta(hours = 2), 
    "H3": timedelta(hours = 3), 
    "H4": timedelta(hours = 4), 
    "H6": timedelta(hours = 6), 
    "H8": timedelta(hours = 8), 
    "H12": timedelta(hours = 12), 
    "D": timedelta(days = 1), 
    "W": timedelta(weeks = 1),
    "M": timedelta(weeks = 4)
}

def clean_time(x):
    return datetime.strptime(
        re.findall("....-..-..T..:..:..", x)[0].replace("T", " "), "%Y-%m-%d %H:%M:%S"
    )

def request_tickers_info(
    symbol: str, 
    from_date: str, 
    granularity: str = "M5"):

    """
    http://developer.oanda.com/rest-live-v20/instrument-ep/
    
    ### Parameters 
    -------------
    
    from -> DateTime [int]                      : The start of the time range to fetch candlesticks for.
                                                  The Unix representation representing the number of seconds since the Unix Epoch (January 1st, 1970 at UTC). 
    
    to   -> DateTime [int]                      : The end of the time range to fetch candlesticks for.
                                                  The Unix representation representing the number of seconds since the Unix Epoch (January 1st, 1970 at UTC). 

    price -> PricingComponent [str]             : The Price component(s) to get candlestick data for. 
                                                  Can contain any combination of the characters “M” (midpoint candles) “B” (bid candles) and “A” (ask candles).
                                                  [default=M] 
                                                  
    granularity -> CandlestickGranularity [str] : The granularity of the candlesticks to fetch 
                                                  S5 [5 sec] | S10 [10 sec] | S15 [15 sec] | S30 [30 sec]
                                                  M1 [1 min] | M2 [2 min] | M4 [4 min] | M5 [5 min] | M10 [10 min] | M15 [15 min] | M30 [30 min]
                                                  H1 [1 hour]| H2 [2 hour]| H3 [3 hour]| H4 [4 hour]| H6 [6 hour]| H8 [8 hour]| H12 [12 hour]
                                                  D [1 day] | W [1 week] | M [1 month]
                                                  [default=S5]
                                                    
    count -> integer [int]                      : The number of candlesticks to return in the response. 
                                                  Count should not be specified if both the start and end parameters are provided, as the time range combined with the granularity will determine the number of candlesticks to return. 
                                                  [default=500, maximum=5000]
    
    smooth -> boolean [bool]                    : A flag that controls whether the candlestick is “smoothed” or not. 
                                                  A smoothed candlestick uses the previous candle’s close price as its open price, while an un-smoothed candlestick uses the first price from its time range as its open price. 
                                                  [default=False]
    
    includeFirst -> boolean [bool]              : A flag that controls whether the candlestick that is covered by the from time should be included in the results. This flag enables clients to use the timestamp of the last completed candlestick received to poll for future candlesticks but avoid receiving the previous candlestick repeatedly. 
                                                  [default=True]

    dailyAlignment -> integer [int]             : The hour of the day (in the specified timezone) to use for granularities that have daily alignments. 
                                                  [default=17, minimum=0, maximum=23]
    
    alignmentTimezone -> string [str]           : The timezone to use for the dailyAlignment parameter. Candlesticks with daily alignment will be aligned to the dailyAlignment hour within the alignmentTimezone. Note that the returned times will still be represented in UTC. 
                                                  [default=America/New_York]
    
    weeklyAlignment -> WeeklyAlignment [str]    : The day of the week used for granularities that have weekly alignment. 
                                                  Monday Tuesday Wednesday Thursday Friday Saturday Sunday
                                                  [default=Friday] 
    """
    base_endpoint = f"https://api-fxtrade.oanda.com"
    candles_endpoint = f"/v3/instruments/{symbol}/candles"
    url = base_endpoint + candles_endpoint

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {account_bearer_token}",
    }
    
    def single_historical_data_request(from_date: str, to_date: str, granularity: str = granularity):
        
        params = {

            "from": int(from_date.timestamp()),
            "to": int(to_date.timestamp()),
            "granularity": granularity
        }
        
        r = requests.get(url, params=params, headers=headers)
        data = r.json()
        
        return list(
            map(
                lambda x: {
                    "date": clean_time(x["time"]),
                    "vol": x["volume"],
                    "open": x["mid"]["o"],
                    "high": x["mid"]["h"],
                    "low": x["mid"]["l"],
                    "close": x["mid"]["c"],
                },
                data["candles"],
            )
        )
    
    todays_date_string = datetime.today().strftime("%Y-%m-%d")
    todays_date = datetime.strptime(todays_date_string, "%Y-%m-%d") 
    
    c_start = datetime.strptime(from_date, "%Y-%m-%d")
    c_end = todays_date
    
    interval = granularities_mapping[granularity]

    entries = np.ceil((c_end - c_start)/interval)

    dates_to_scrape = []

    for chunk in range(int(np.ceil(entries / 5000))):
        c_end = c_start + interval * 5000 
        c_end = todays_date if c_end > todays_date else c_end
        df = single_historical_data_request(c_start, c_end)
        print(f"Retrieved data for dates {c_start} to {c_end}, dataframe shape is {len(df)}")
        dates_to_scrape.append(df)
        c_start += interval * 5000
        if chunk == 2: break
    
    return dates_to_scrape