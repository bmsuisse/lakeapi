import os

# In this file we have parameter that can only be set using environment variables.

CACHE_EXPIRATION_TIME_SECONDS = int(os.getenv("CACHE_EXPIRATION_TIME_SECONDS", 3200))
CACHE_TYPE = os.getenv("CACHE_TYPE", "auto")

prefixes = ["men", "redis", "disk"]

if not any(CACHE_TYPE.startswith(prefix) for prefix in prefixes):
    CACHE_TYPE = "auto"
