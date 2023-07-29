from fastapi import Depends, FastAPI, Request

from bmsdna.lakeapi.api.api import init_lakeapi
from cashews import cache
from cashews.commands import Command
import time


def run_fastapi():
    app = FastAPI()
    sti = init_lakeapi(app, use_basic_auth=True)

    @app.get("/")
    async def root(req: Request):
        return {"User": req.user["username"]}

    # caching middleware
    # https://github.com/Krukov/cashews/blob/master/examples/fastapi_app.py
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

    return app


app = run_fastapi()
