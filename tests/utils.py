from fastapi import Depends, FastAPI, Request
import dataclasses
from faker import Faker
import random
import os


def get_app(default_engine="duckdb"):
    import bmsdna.lakeapi

    os.environ["MY_SQL_PWD"] = "MyPass@word4tests"
    app = FastAPI()
    def_cfg = bmsdna.lakeapi.get_default_config()
    cfg = dataclasses.replace(def_cfg, enable_sql_endpoint=True, data_path="tests/data", default_engine=default_engine)
    sti = bmsdna.lakeapi.init_lakeapi(app, True, cfg, "config_test.yml")

    @app.get("/")
    async def root(req: Request):
        return {"User": req.user["username"]}

    return app


def get_auth():
    user = "test"
    pw = "B~C:BB*_9-1u"
    return (user, pw)


def random_coords(fake: Faker):
    c = fake.local_latlng("CH")
    return {"geo_lat": float(c[0]), "geo_lon": float(c[1])}


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
        | random_coords(fake)
        for x in range(num)
    ]
    return output
