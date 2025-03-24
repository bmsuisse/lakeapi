import inspect
from typing import Awaitable, TypeVar

T = TypeVar("T")


async def _async(thing: T | Awaitable[T]) -> T:
    if inspect.isawaitable(thing):
        return await thing
    return thing
