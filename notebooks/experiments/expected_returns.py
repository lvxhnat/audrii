import constants as const
import numpy as np 
import utilities

def _historical_returns(
    arr: np.ndarray,
    ctype: str = "log"):
    
    """ Calculate the returns given prices
    
    Params
    ----------
    arr       : A one dimensional numpy array containing the closing prices of the asset 
    ctype     : The return calculation type. Available selections are: log, 
    
    """
    
    assert arr.ndim == 1, "Pass in only one dimensional np arrays"
    
    if ctype == "log":
        return utilities.pad_array(np.log(1 + np.diff(arr)/arr[1:]), arr.size)
    else: 
        raise ValueError("Please enter a valid ctype: log")
        
def mean_historical_returns(
    arr: np.ndarray, 
    days: int,
    compound: bool = True, 
    ):
    
    """ Calculate mean returns over a time period. 
    
    Params
    ----------
    arr           : A one dimensional numpy array containing the closing prices of the asset.
    days          : The number of days of data the arr time series represent
    compound      : If True, calculates the Geometric Mean. Else calculates the Arithmetic Mean.
    
    """
    hist_rets = _historical_returns(arr)
    hist_rets = hist_rets[np.logical_not(np.isnan(hist_rets))] ## Remove nan values
    
    if compound: 
        return np.exp(np.log(hist_rets).mean()) ## In case there is overflow
    
def annualised_daily_mean_returns(
    arr: np.ndarray, 
    days: int,
    geometric: bool = True, 
    ):

    """ Calculate the annualised daily mean returns over a time period. 
    
    Params
    ----------
    arr           : A one dimensional numpy array containing the closing prices of the asset.
    days          : The number of days of data the arr time series represent
    geometric      : If True, calculates the Geometric Mean. Else calculates the Arithmetic Mean.
    
    """
    
    hist_rets = _historical_returns(arr)
    hist_rets = hist_rets[np.logical_not(np.isnan(hist_rets))] ## Remove nan values
    
    if geometric:
        ## Here we add 1 to prevent missing point numbers. 
        return ((1 + hist_rets).prod() ** (1.0 / len(hist_rets)) - 1) * (days / const.trading_days_per_year)
    else:  
        return hist_rets.mean() * (days / const.trading_days_per_year)
    