"""yt-tracker time function logger"""

import time
from yt_profile_stats.yt_logger.logger import yt_logger

def time_it(func):
    """
    Get total time a module ran
    """

    def wrapper(*args, **kwargs):
        """"
        Wrapper class
        """
        start_time = time.time()
        func_name = func.__name__
        result = func(*args, **kwargs)
        end_time = time.time()
        message = f'Function {func_name} took {round(end_time - start_time, 2)} seconds to run'
        yt_logger.info(message)
        return result

    return wrapper
