from fastapi import Depends, FastAPI

from bmsdna.lakeapi.api.api import init_lakeapi


def run_fastapi():
    app = FastAPI()
    sti = init_lakeapi(app)

    @app.get("/")
    async def root(username: str = Depends(sti.get_username)):
        return {"User": username}

    return app


app = run_fastapi()
