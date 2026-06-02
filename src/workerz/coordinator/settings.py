import os
from uuid import uuid4

from dotenv import load_dotenv


class Settings:
    def __init__(self):
        load_dotenv()
        self.tcp_host  = os.getenv("WORKERZ_TCP_HOST", "0.0.0.0")
        self.tcp_port  = int(os.getenv("WORKERZ_TCP_PORT", 7777))
        self.ping_interval = int(os.getenv("WORKERZ_PING_INTERVAL", 10))
        self.pong_timeout  = int(os.getenv("WORKERZ_PONG_TIMEOUT", 15))
        self.log_file  = os.getenv("WORKERZ_COORDINATOR_LOG", "./logs/coordinator.log")
        self.log_level = os.getenv("WORKERZ_LOG_LEVEL", "INFO")
        self.build_version = str(uuid4())[:8]


settings = Settings()
