import os

# In this file we have parameter that can only be set using environment variables.

CACHE_EXPIRATION_TIME_SECONDS = int(os.getenv("CACHE_EXPIRATION_TIME_SECONDS", 43200))
