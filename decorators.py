import threading, logging
from datetime import datetime
from functools import wraps


def threader(func):

    @wraps(func)
    def run(*args, **kwargs):
        thread = threading.Thread(target=func, args=args, kwargs=kwargs)
        thread.start()
        return thread
    return run

def logger(func):

    @wraps(func)
    def log_this(*args,**kwargs):
        print(func.__name__)
        logging.basicConfig(filename="./basic_log.txt", level=logging.DEBUG)
        logging.info(f"{datetime.now()} attempt to run function named {func.__name__}")
        func()

    return log_this
