import os
from typing import Final

CACHE_EXPIRATION_TIME_SECONDS = int(os.getenv("CACHE_EXPIRATION_TIME_SECONDS", 43200))

CONFIG_PATH = os.getenv("CONFIG_PATH", "config.yml")
TEMP_FOLDER_PATH = os.getenv("TEMP_FOLDER_PATH", "temp")
DATA_PATH = os.environ["DATA_PATH"] if "DATA_PATH" in os.environ else "data"

print(f"DATA_PATH: {DATA_PATH}")


IS_CACHING_ENABLED_GLOBALLY: Final[bool] = CACHE_EXPIRATION_TIME_SECONDS > 0
IS_FILE_CACHE: Final[bool] = False


JWT_SECRET = os.getenv("JWT_SECRET", None)  # None disables the token feature
