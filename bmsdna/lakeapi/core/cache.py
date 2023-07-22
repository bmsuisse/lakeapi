from bmsdna.lakeapi.core.env import CACHE_EXPIRATION_TIME_SECONDS, CACHE_TYPE


def is_cache(result, args, kwargs, key=None):
    if CACHE_EXPIRATION_TIME_SECONDS <= 0:
        return False
    return True
