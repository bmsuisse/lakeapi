from typing import List, Sequence
from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from bmsdna.lakeapi.core.config import BasicConfig, Configs, UserConfig
from bmsdna.lakeapi.core.yaml import get_yaml
import inspect
from aiocache import cached, Cache
from aiocache.serializers import PickleSerializer
from bmsdna.lakeapi.core.env import CACHE_EXPIRATION_TIME_SECONDS
from fastapi import FastAPI, Response

cache = cached(
    ttl=CACHE_EXPIRATION_TIME_SECONDS * 10000,
    cache=Cache.MEMORY,
    serializer=PickleSerializer(),
)

security = HTTPBasic()


userhashmap: dict[str, str] | None = None


@cache
async def is_correct(hash: str, pwd_str: str):
    import argon2

    ph = argon2.PasswordHasher()
    return ph.verify(hash.encode("utf-8"), pwd_str.encode("utf-8"))


def add_user_middlware(app: FastAPI, basic_config: BasicConfig, users: Sequence[UserConfig]):
    @app.middleware("http")
    async def get_username(request: Request, call_next):
        import json

        credentials = await HTTPBasic(auto_error=False)(request)
        if credentials is None:
            return Response(
                status_code=401,
                headers={"WWW-Authenticate": "Basic"},
                content=json.dumps({"detail": "Not authenticated"}),
            )

        global userhashmap
        userhashmap = userhashmap or {
            ud["name"].casefold(): ud["passwordhash"] for ud in users if ud["name"]
        }  # pay attention not to include an empty user by accident

        if not credentials.username.casefold() in userhashmap.keys():
            return Response(
                status_code=401,
                headers={"WWW-Authenticate": "Basic"},
                content=json.dumps({"detail": "Incorrect email or password"}),
            )
        pwd_str = credentials.password
        hash = userhashmap[credentials.username.casefold()]

        is_correct_password = is_correct(hash, pwd_str)
        if not isinstance(is_correct_password, bool):
            is_correct_password = await is_correct_password
        if not is_correct_password:
            return Response(
                status_code=401, headers={"WWW-Authenticate": "Basic"}, content="Incorrect email or password"
            )
        request.scope["user"] = {"username": credentials.username}
        return await call_next(request)
