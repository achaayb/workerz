import json
import os
from contextlib import asynccontextmanager
from pathlib import Path

import redis.asyncio as aioredis
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse
from starlette.routing import Route

REDIS_URL = os.environ.get("WORKERZ_REDIS_URL", "redis://localhost:6379")
HTTP_PORT = int(os.environ.get("WORKERZ_DASHBOARD_PORT", 8080))

rdb: aioredis.Redis = None


async def route_dashboard(request: Request):
    html = (Path(__file__).parent / "index.html").read_text()
    return HTMLResponse(html)


async def route_jobs(request: Request):
    ids  = await rdb.smembers("workerz:jobs")
    jobs = []
    for jid in ids:
        raw = await rdb.hgetall(f"workerz:job:{jid}")
        if raw:
            jobs.append({
                "id":          raw["id"],
                "task":        raw["task"],
                "labels":      json.loads(raw.get("labels", "[]")),
                "status":      raw["status"],
                "worker_id":   raw.get("worker_id") or None,
                "result":      json.loads(raw["result"]) if raw.get("result") else None,
                "error":       raw.get("error") or None,
                "warnings":    json.loads(raw.get("warnings", "[]")),
                "infos":       json.loads(raw.get("infos", "[]")),
                "debug":       json.loads(raw.get("debug", "[]")),
                "meta":        json.loads(raw.get("meta", "{}")),
                "created_at":  raw["created_at"],
                "finished_at": raw.get("finished_at") or None,
            })
    jobs.sort(key=lambda j: j["created_at"], reverse=True)
    return JSONResponse(jobs)


async def route_workers(request: Request):
    ids     = await rdb.smembers("workerz:workers")
    workers = []
    for wid in ids:
        raw = await rdb.hgetall(f"workerz:worker:{wid}")
        if raw:
            workers.append({
                "id":          raw["id"],
                "labels":      json.loads(raw.get("labels", "[]")),
                "status":      raw.get("status", "offline"),
                "current_job": raw.get("current_job") or None,
                "last_seen":   raw.get("last_seen"),
            })
    return JSONResponse(workers)


@asynccontextmanager
async def lifespan(app):
    global rdb
    rdb = aioredis.from_url(REDIS_URL, decode_responses=True)
    print(f"dashboard  http://0.0.0.0:{HTTP_PORT}  redis={REDIS_URL}")
    yield
    await rdb.aclose()


app = Starlette(
    lifespan=lifespan,
    routes=[
        Route("/",        route_dashboard),
        Route("/jobs",    route_jobs),
        Route("/workers", route_workers),
    ],
)
