from fastapi import Depends, FastAPI, Request

from bmsdna.lakeapi.api.api import init_lakeapi


def run_fastapi():
    app = FastAPI()
    sti = init_lakeapi(app, use_basic_auth=True)

    @app.get("/")
    async def root(req: Request):
        return {"User": req.user["username"]}

    return app


app = run_fastapi()
