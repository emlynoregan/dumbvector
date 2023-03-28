import time

def time_function(func, message=None):
    def timed(*args, **kw):
        ts = time.time()
        try:
            result = func(*args, **kw)
        finally:
            te = time.time()

            print(f'{message or func.__name__}  {te-ts:.2f} sec')
        return result

    return timed