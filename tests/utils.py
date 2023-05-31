from fastapi import Depends, FastAPI
import dataclasses
import os
from faker import Faker
import random


def get_app(default_engine="duckdb"):
    import bmsdna.lakeapi

    app = FastAPI()
    def_cfg = bmsdna.lakeapi.get_default_config()
    cfg = dataclasses.replace(def_cfg, enable_sql_endpoint=True, data_path="tests/data", default_engine=default_engine)
    sti = bmsdna.lakeapi.init_lakeapi(app, cfg, "config_test.yml")

    @app.get("/")
    async def root(username: str = Depends(sti.get_username)):
        return {"User": username}

    return app


def get_auth():
    user = "test"
    pw = "B~C:BB*_9-1u"
    return (user, pw)


def create_rows_faker(num=1):
    fake = Faker()
    output = [
        {
            "name": fake.name(),
            "address": fake.address(),
            "name": fake.name(),
            "email": fake.email(),
            "bs": fake.bs(),
            "city": fake.city(),
            "state": fake.state(),
            "date_time": fake.date_time(),
            "paragraph": fake.paragraph(),
            "Conrad": fake.catch_phrase(),
            "randomdata": random.randint(1000, 2000),
            "abc": random.choice(["a", "b", "c"]),
        }
        for x in range(num)
    ]
    return output
