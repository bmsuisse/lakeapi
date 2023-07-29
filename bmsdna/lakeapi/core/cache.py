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


def update_header():
    # https://github.com/long2ice/fastapi-cache/blob/main/fastapi_cache/decorator.py#L197
    ...
