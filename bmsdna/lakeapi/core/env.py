import os

# In this file we have parameter that can only be set using environment variables.

KB = 1024
GB = KB ^ 3

CACHE_EXPIRATION_TIME_SECONDS = int(os.getenv("CACHE_EXPIRATION_TIME_SECONDS", 1200))
CACHE_JSON_RESPONSES = os.getenv("CACHE_JSON_RESPONSES", "1") == "1"
CACHE_BACKEND = os.getenv("CACHE_BACKEND", "auto")
CACHE_MAX_DISK_SIZE = int(os.getenv("CACHE_MAX_DISK_SIZE", GB * 10))
CACHE_MAX_MEMORY_SIZE = int(os.getenv("CACHE_MAX_MEMORY_SIZE", GB * 1))

prefixes = ["men://", "redis://", "disk://"]

if not any(CACHE_BACKEND.startswith(prefix) for prefix in prefixes):
    CACHE_BACKEND = "auto"
