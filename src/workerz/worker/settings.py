import os
from uuid import uuid4

from dotenv import load_dotenv


class Settings:
    def __init__(self):
        load_dotenv()
        self.coordinator_host = os.getenv("WORKERZ_COORDINATOR_HOST", "127.0.0.1")
        self.coordinator_tcp  = int(os.getenv("WORKERZ_COORDINATOR_TCP", 7777))
        self.labels           = [
            l.strip() for l in os.getenv("WORKERZ_LABELS", "").split(",") if l.strip()
        ]
        self.log_file  = os.getenv("WORKERZ_WORKER_LOG", "./logs/worker.log")
        self.log_level = os.getenv("WORKERZ_LOG_LEVEL", "INFO")
        self.build_version = str(uuid4())[:8]

settings = Settings()
