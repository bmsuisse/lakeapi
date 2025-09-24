from fastapi import FastAPI, Request, status
import dataclasses
from faker import Faker
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
import random
import os
import logging


def get_app(default_engine="duckdb"):
    import bmsdna.lakeapi

    os.environ["MY_SQL_PWD"] = "MyPass@word4tests"
    app = FastAPI()
    def_cfg = bmsdna.lakeapi.get_default_config()
    cfg = dataclasses.replace(
        def_cfg,
        enable_sql_endpoint=True,
        data_path="tests/data",
        default_engine=default_engine,
    )

    bmsdna.lakeapi.init_lakeapi(app, True, cfg, "config_test.yml")

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(
        request: Request, exc: RequestValidationError
    ):
        exc_str = f"{exc}".replace("\n", " ").replace("   ", " ")
        logging.error(f"{request}: {exc_str}")
        content = {"status_code": 10422, "message": exc_str, "data": None}
        return JSONResponse(
            content=content, status_code=status.HTTP_422_UNPROCESSABLE_ENTITY
        )

    @app.get("/")
    async def root(req: Request):
        return {"User": req.user["username"]}

    return app


def get_auth():
    user = "test"
    pw = "B~C:BB*_9-1u"
    return (user, pw)


lat1 = 46.7079055
lon1 = 8.6527287

lat2 = 45.752338
lon2 = 11.790724


def create_rows_faker(num=1):
    fake = Faker()
    output = [
        {
            "name": fake.name(),
            "address": fake.address(),
            "email": fake.email(),
            "bs": fake.bs(),
            "city": fake.city(),
            "state": fake.state(),
            "date_time": fake.date_time(),
            "paragraph": fake.paragraph(),
            "Conrad": fake.catch_phrase(),
            "randomdata": random.randint(1000, 2000),
            "abc": random.choice(["a", "b", "c"]),
            "geo_lat": lat1
            + (lat1 - lat2) / num * x,  # not really random, but good to test
            "geo_lon": lon1 + (lon2 - lon1) / num * x,
        }
        for x in range(num)
    ]
    return output
