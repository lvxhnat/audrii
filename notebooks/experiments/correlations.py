import numpy as np 
import pandas as pd

from . import utilities

def rolling_spearman_corr(
    arr_0: np.array, 
    arr_1: np.array, 
    window: int):
    
    """ Optimised Calculation for Rolling Pairwise Spearmanr correlation
    
    Params 
    ----------
    arr_0     : The first 1D array to calculate spearman correlation for
    arr_1     : The second 1D array to calculate spearman correlation for
    window    : The number of n days to calculate spearman correlation for
    
    Credits: This function is adapted from https://stackoverflow.com/questions/48186624/pandas-rolling-window-spearman-correlation
    
    """
    if arr_0.ndim > 1 or arr_1.ndim > 1:
        raise ValueError("Please ensure your input arrays are 1D.")
        
    stride_0 = arr_0.strides[0]
    stride_1 = arr_0.strides[0]
    
    ss_0 = np.lib.stride_tricks.as_strided(arr_0, shape=[len(arr_0) - window + 1, window], strides=[stride_0, stride_0])
    ss_1 = np.lib.stride_tricks.as_strided(arr_1, shape=[len(arr_1) - window + 1, window], strides =[stride_1, stride_1])

    corrs = pd.DataFrame(ss_0).rank(1).corrwith(pd.DataFrame(ss_1).rank(1), 1)
    
    return utilities.pad_array(corrs, arr_0.size)

def pearson_corr_matrix(arr: np.array) -> np.ndarray:
    
    """ Given a 2D numpy matrix, calculate its pearson correlation matrix
    
    Params 
    ----------
    arr       : The 2D array candidate for the matrix calculation
    
    Credits: This function is adapted from https://stackoverflow.com/questions/30143417/computing-the-correlation-coefficient-between-two-multi-dimensional-arrays
    """
    
    arr_marr = arr - arr.mean(1)[:, None] # Row-wise mean of input arrays & subtract from input arrays themselves
    ss_arr = (arr_marr ** 2).sum(1) # Row-wise sum of squares 
    
    return np.dot(arr_marr, arr_marr.T) / np.sqrt(np.dot(ss_arr[:, None], ss_arr[None]))

def spearman_corr_matrix(arr: np.array) -> np.ndarray:
    
    """ Given a 2D numpy matrix, calculate its spearman correlation matrix
    Spearman Correlation is simply pearson correlation of the ranked matrix.
    
    Params 
    ----------
    arr       : The 2D array candidate for the matrix calculation
    """
    ranked_candidate_matrix = np.array([utilities.rank(a) for a in arr])
    
    return pearson_corr_matrix(ranked_candidate_matrix)