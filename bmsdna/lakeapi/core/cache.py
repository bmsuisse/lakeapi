from bmsdna.lakeapi.core.env import (
    CACHE_BACKEND,
    CACHE_EXPIRATION_TIME_SECONDS,
    CACHE_JSON_RESPONSES,
    CACHE_MAX_DISK_SIZE,
    CACHE_MAX_MEMORY_SIZE,
)
from fastapi import FastAPI
import time
from cashews import cache
from cashews.commands import Command


def get_max_cache_size(disk=True):
    if disk:
        return CACHE_MAX_DISK_SIZE
    return CACHE_MAX_MEMORY_SIZE


def add_cache_middleware(app: FastAPI):
    @app.middleware("http")
    async def add_process_time_header(request, call_next):
        start_time = time.perf_counter()
        response = await call_next(request)
        process_time = time.perf_counter() - start_time
        response.headers["X-Process-Time"] = str(process_time)
        return response

    @app.middleware("http")
    async def add_from_cache_headers(request, call_next):
        with cache.detect as detector:
            response = await call_next(request)
            if request.method.lower() != "get":
                return response
            if detector.calls:
                response.headers["X-From-Cache-keys"] = ";".join(detector.calls.keys())
        return response

    @app.middleware("http")
    async def disable_middleware(request, call_next):
        if request.headers.get("X-No-Cache"):
            with cache.disabling(Command.GET):
                return await call_next(request)
        return await call_next(request)
