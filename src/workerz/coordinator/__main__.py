import os, uvicorn
from workerz.coordinator.app import app
uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("WORKERZ_HTTP_PORT", 8000)), log_level="info")
