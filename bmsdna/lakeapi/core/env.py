import os

# In this file we have parameter that can only be set using environment variables.


CACHE_EXPIRATION_TIME_SECONDS = int(os.getenv("CACHE_EXPIRATION_TIME_SECONDS", 1200))
CACHE_BACKEND = os.getenv("CACHE_BACKEND", "auto")

prefixes = ["men://", "redis://", "disk://"]

if not any(CACHE_BACKEND.startswith(prefix) for prefix in prefixes):
    CACHE_BACKEND = "auto"
