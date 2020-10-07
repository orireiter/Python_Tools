import threading, logging
from datetime import datetime
from functools import wraps


# takes a given function and returns it as a thread
def threader(func):

    @wraps(func)
    def run(*args, **kwargs):
        thread = threading.Thread(target=func, args=args, kwargs=kwargs)
        thread.start()
        return thread
    return run

# logs an execution of a function, then executes, also returns for later refernce
def logger(func):

    @wraps(func)
    def log_this(*args,**kwargs):
        logging.basicConfig(filename="./basic_log.txt", level=logging.DEBUG)
        logging.info(f"{datetime.now()} attempt to run function named {func.__name__}")
        func()
        return(func)

    return log_this
