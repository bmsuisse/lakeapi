from fastapi import Depends, FastAPI

import os


def get_app():
    import bmsdna.lakeapi.core.route
    from bmsdna.lakeapi.core.config import Configs

    configs: Configs = Configs.from_yamls(os.getenv("CONFIG_PATH", "config.yml"))
    router = bmsdna.lakeapi.core.route.init_routes(configs)
    app = FastAPI()
    app.include_router(router)

    from bmsdna.lakeapi.core.uservalidation import get_current_username

    @app.get("/")
    async def root(username: str = Depends(get_current_username)):
        return {"User": username}

    return app


def get_auth():
    user = "test"
    pw = "B~C:BB*_9-1u"
    return (user, pw)
