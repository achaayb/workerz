import os
from uuid import uuid4

from dotenv import load_dotenv


class Settings:
    def __init__(self):
        load_dotenv()
        self.coordinator_host = os.getenv("WORKERZ_COORDINATOR_HOST", "127.0.0.1")
        self.coordinator_tcp  = int(os.getenv("WORKERZ_COORDINATOR_TCP", 7777))
        self.log_file  = os.getenv("WORKERZ_SDK_LOG", "./logs/sdk.log")
        self.log_level = os.getenv("WORKERZ_LOG_LEVEL", "INFO")
        self.build_version = str(uuid4())[:8]


settings = Settings()
