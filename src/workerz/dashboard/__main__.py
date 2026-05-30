import os, uvicorn
from workerz.dashboard.app import app
uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("WORKERZ_DASHBOARD_PORT", 8080)), log_level="info")
