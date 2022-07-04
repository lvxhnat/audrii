import numpy as np 

def rank(
    arr: np.array):
    
    """ Rank a 1D numpy array to ordinal values. 
    This function is slightly faster than the application of rankdata in numpy by removing type and parameter checks.
    
    Parameters
    ----------
    arr       : The numpy 1D array to calculate ranks for
    """
    
    sorter = np.argsort(arr, kind='quicksort')
    
    inv = np.empty(sorter.size, dtype=np.intp)
    inv[sorter] = np.arange(sorter.size, dtype=np.intp)

    arr = arr[sorter]
    obs = np.r_[True, arr[1:] != arr[:-1]]
    dense = obs.cumsum()[inv]

    # cumulative counts of each unique value
    count = np.r_[np.nonzero(obs)[0], len(obs)]

    # average method
    return .5 * (count[dense] + count[dense - 1] + 1)

def pad_array(arr: np.ndarray, size: tuple):
    """ Pad a 1D numpy array to match size requirements
    
    Parameters
    ----------
    arr       : The numpy 1D array to pad left for
    
    """
    repad_size = abs(size - arr.size)
    return np.pad(arr, (repad_size, 0), mode = 'constant', constant_values = (np.nan,))

