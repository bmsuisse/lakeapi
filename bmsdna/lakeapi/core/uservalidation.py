from typing import List, Sequence
from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from bmsdna.lakeapi.core.config import BasicConfig, Configs, UserConfig
from bmsdna.lakeapi.core.yaml import get_yaml
import inspect
from aiocache import cached, Cache
from aiocache.serializers import PickleSerializer
from bmsdna.lakeapi.core.env import CACHE_EXPIRATION_TIME_SECONDS

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


async def get_username(req: Request, basic_config: BasicConfig, users: Sequence[UserConfig]):
    if req.query_params.get("token") and basic_config.token_jwt_secret is not None:
        import jwt

        token = req.query_params["token"]
        dt = jwt.decode(token, basic_config.token_jwt_secret, algorithms=["HS256"])
        if dt["path"] == req.url.path or dt["host"] == req.url.hostname:
            return dt["username"]
    credentials = await HTTPBasic(auto_error=True)(req)
    assert credentials is not None
    global userhashmap
    userhashmap = userhashmap or {
        ud["name"].casefold(): ud["passwordhash"] for ud in users if ud["name"]
    }  # pay attention not to include an empty user by accident

    if not credentials.username.casefold() in userhashmap.keys():
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    pwd_str = credentials.password
    hash = userhashmap[credentials.username.casefold()]

    is_correct_password = is_correct(hash, pwd_str)
    if not isinstance(is_correct_password, bool):
        is_correct_password = await is_correct_password
    if not is_correct_password:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username
