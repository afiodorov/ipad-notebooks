import pickle

from datetime import datetime, timedelta
from functools import wraps
from hashlib import sha224
from pathlib import Path
from time import time
from typing import Optional


def cache(folder: str = "mylib/tmp", expiration: Optional[timedelta] = timedelta(days=20)):
    """
    Caches execution of a function for a specified period to disk.
    Inspired by `functools.lru_cache`.
    Examples:
        client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
        @cache(folder='.cache') # caches for a day into the folder .cache in current dir
        def bigquery(query):
            return client.query(query).to_dataframe()
        @cache(expiration=None) # caches indefinately into /tmp
        def bigquery(query):
            return client.query(query).to_dataframe()
        You can clear the cache by running `rm -rf /tmp/cache_*`.
    """
    dir_ = Path(folder)
    dir_.mkdir(parents=True, exist_ok=True)

    def decorator(func):
        @wraps(func)
        def with_caching(*args, **kwargs):
            clear_cache = kwargs.get("_clear_cache")
            if "_clear_cache" in kwargs:
                del kwargs["_clear_cache"]

            cache_only = kwargs.get("_cache_only")
            if "_cache_only" in kwargs:
                del kwargs["_cache_only"]

            arg_str = [str(a) for a in args]
            kwargs_str = [f"{k}={v}" for k, v in kwargs.items()]
            function_call = (
                f"{func.__module__}.{func.__name__}({', '.join(arg_str + kwargs_str)})"
            )
            args_encoded = sha224(function_call.encode("utf-8")).hexdigest()
            file_name = f"cache_{func.__name__}_{args_encoded}"
            file_path = dir_ / file_name
            if not file_path.exists():
                if cache_only:
                    return None

                ret = func(*args, **kwargs)
                with file_path.open(mode="wb") as f:
                    pickle.dump(ret, f)
                return ret

            if clear_cache:
                if cache_only:
                    return None

                ret = func(*args, **kwargs)
                with file_path.open(mode="wb") as f:
                    pickle.dump(ret, f)
                return ret

            if expiration:
                now = datetime.fromtimestamp(time())
                mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
                elapsed = now - mtime
                if elapsed >= expiration:
                    ret = func(*args, **kwargs)
                    with file_path.open(mode="wb") as f:
                        pickle.dump(ret, f)
                    return ret

            with file_path.open(mode="rb") as f:
                return pickle.load(f)

        return with_caching

    return decorator