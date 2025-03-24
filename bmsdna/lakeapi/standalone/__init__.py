from fastapi import Depends, FastAPI, Request
import asyncio
from bmsdna.lakeapi.api.api import init_lakeapi


def run_fastapi():
    app = FastAPI()

    async def _init():
        await init_lakeapi(app, use_basic_auth=True)

    asyncio.run(_init())

    @app.get("/")
    async def root(req: Request):
        return {"User": req.user["username"]}

    return app


app = run_fastapi()
