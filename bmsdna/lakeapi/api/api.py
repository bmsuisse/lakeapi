from fastapi import FastAPI


def init_lakeapi(app: FastAPI):
    configs: Configs = Configs.from_yamls(os.getenv("CONFIG_PATH", "config.yml"))
    router = bmsdna.lakeapi.core.route.init_routes(configs)
    app.include_router(router)
