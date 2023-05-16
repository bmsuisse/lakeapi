from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
import argon2
from bmsdna.lakeapi.core.yaml import get_yaml
import inspect
from aiocache import cached, Cache
from aiocache.serializers import PickleSerializer
from bmsdna.lakeapi.core.env import CACHE_EXPIRATION_TIME_SECONDS, CONFIG_PATH, DISABLE_BASIC_AUTH, JWT_SECRET


cache = cached(
    ttl=CACHE_EXPIRATION_TIME_SECONDS * 10000,
    cache=Cache.MEMORY,
    serializer=PickleSerializer(),
)

security = HTTPBasic()


def _readdb():
    if DISABLE_BASIC_AUTH:
        return
    users = get_yaml(CONFIG_PATH).get("users")
    for user in users:
        yield user


userhashmap = {
    ud["name"].casefold(): ud["passwordhash"] for ud in _readdb() if ud["name"]
}  # pay attention not to include an empty user by accident


@cache
async def is_correct(hash: str, pwd_str: str):
    ph = argon2.PasswordHasher()
    return ph.verify(hash.encode("utf-8"), pwd_str.encode("utf-8"))


async def get_current_username(req: Request):
    if req.query_params.get("token") and JWT_SECRET is not None:
        import jwt
        import env

        token = req.query_params["token"]
        dt = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
        if dt["path"] == req.url.path or dt["host"] == req.url.hostname:
            return dt["username"]
    if DISABLE_BASIC_AUTH:
        return None
    credentials = await HTTPBasic(auto_error=True)(req)
    assert credentials is not None
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
