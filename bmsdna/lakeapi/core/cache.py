from bmsdna.lakeapi.core.env import (
    CACHE_BACKEND,
    CACHE_EXPIRATION_TIME_SECONDS,
    CACHE_JSON_RESPONSES,
    CACHE_MAX_DISK_SIZE,
    CACHE_MAX_MEMORY_SIZE,
)


def get_max_cache_size(disk=True):
    if disk:
        return CACHE_MAX_DISK_SIZE
    return CACHE_MAX_MEMORY_SIZE


def is_cache(result, args, kwargs, key=None):
    if CACHE_EXPIRATION_TIME_SECONDS <= 0:
        return False
    return True


def is_cache_json_response(result, args, kwargs, key=None):
    if CACHE_EXPIRATION_TIME_SECONDS <= 0 or not CACHE_JSON_RESPONSES:
        return False
    return kwargs.get("format") == "json" or kwargs.get("request").headers.get("Accept") == "application/json"
