"""
Coordinator — stateless re: jobs (all job state lives in Redis).
In-memory: only TCP writer handles.

HTTP routes:
  POST   /job          submit job
  GET    /job/{id}     get job (status + result envelope)
  DELETE /job/{id}     cancel job

TCP: workers connect, coordinator pushes Dispatch messages.
"""

import asyncio
import json
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import redis.asyncio as aioredis
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from workerz.protocol import (
    Cancel, Dispatch, JobStatus, JobUpdate, Ping, Pong, Register,
    recv_msg, send_msg,
)

TCP_HOST  = os.environ.get("WORKERZ_TCP_HOST", "0.0.0.0")
TCP_PORT  = int(os.environ.get("WORKERZ_TCP_PORT", 7777))
HTTP_PORT = int(os.environ.get("WORKERZ_HTTP_PORT", 8000))
REDIS_URL = os.environ.get("WORKERZ_REDIS_URL", "redis://localhost:6379")

PING_INTERVAL = 10
PONG_TIMEOUT  = 15

# Only thing kept in memory: writer handles keyed by worker_id
_writers: dict[str, asyncio.StreamWriter] = {}

rdb: aioredis.Redis  = None
queue: asyncio.Queue = None


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── Redis helpers ─────────────────────────────────────────────────────────────

async def _save_worker(worker_id: str, labels: list[str], status: str, current_job: str = ""):
    await rdb.sadd("workerz:workers", worker_id)
    await rdb.hset(f"workerz:worker:{worker_id}", mapping={
        "id":          worker_id,
        "labels":      json.dumps(labels),
        "status":      status,
        "current_job": current_job,
        "last_seen":   _now(),
    })


async def _delete_worker(worker_id: str):
    await rdb.srem("workerz:workers", worker_id)
    await rdb.delete(f"workerz:worker:{worker_id}")


async def _save_job(job: dict):
    await rdb.sadd("workerz:jobs", job["id"])
    await rdb.hset(f"workerz:job:{job['id']}", mapping={
        "id":          job["id"],
        "task":        job["task"],
        "args":        json.dumps(job["args"]),
        "kwargs":      json.dumps(job["kwargs"]),
        "labels":      json.dumps(job.get("labels") or []),
        "status":      job["status"],
        "worker_id":   job.get("worker_id") or "",
        "result":      job.get("result") or "",
        "error":       job.get("error") or "",
        "warnings":    json.dumps(job.get("warnings") or []),
        "infos":       json.dumps(job.get("infos") or []),
        "debug":       json.dumps(job.get("debug") or []),
        "meta":        json.dumps(job.get("meta") or {}),
        "created_at":  job["created_at"],
        "finished_at": job.get("finished_at") or "",
    })


async def _load_job(job_id: str) -> dict | None:
    raw = await rdb.hgetall(f"workerz:job:{job_id}")
    if not raw:
        return None
    return {
        "id":          raw["id"],
        "task":        raw["task"],
        "args":        json.loads(raw["args"]),
        "kwargs":      json.loads(raw["kwargs"]),
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
    }


async def _load_all_workers() -> list[dict]:
    ids  = await rdb.smembers("workerz:workers")
    out  = []
    for wid in ids:
        raw = await rdb.hgetall(f"workerz:worker:{wid}")
        if raw:
            out.append({
                "id":          raw["id"],
                "labels":      json.loads(raw.get("labels", "[]")),
                "status":      raw.get("status", "offline"),
                "current_job": raw.get("current_job") or None,
                "last_seen":   raw.get("last_seen"),
            })
    return out


# ── Dispatch ──────────────────────────────────────────────────────────────────

async def _find_idle_worker(labels: list[str]) -> tuple[str, list[str]] | None:
    """Return (worker_id, worker_labels) of an idle worker matching all requested labels, or None."""
    for wid, writer in _writers.items():
        raw = await rdb.hgetall(f"workerz:worker:{wid}")
        if not raw or raw.get("status") != "idle":
            continue
        worker_labels = set(json.loads(raw.get("labels", "[]")))
        if not labels or set(labels).issubset(worker_labels):
            return wid, list(worker_labels)
    return None


async def _dispatch_next():
    """Try to assign queued jobs to idle workers."""
    pending = []
    while not queue.empty():
        job = await queue.get()
        match = await _find_idle_worker(job.get("labels") or [])
        if match:
            wid, _ = match
            await _assign(wid, job)
        else:
            pending.append(job)
    for job in pending:
        await queue.put(job)


async def _assign(worker_id: str, job: dict):
    writer = _writers.get(worker_id)
    if not writer:
        await queue.put(job)
        return

    raw = await rdb.hgetall(f"workerz:worker:{worker_id}")
    labels = json.loads(raw.get("labels", "[]")) if raw else []

    job["status"]    = "dispatched"
    job["worker_id"] = worker_id
    await _save_job(job)
    await _save_worker(worker_id, labels, "busy", job["id"])
    await send_msg(writer, Dispatch(
        job_id=job["id"], task=job["task"],
        args=job["args"], kwargs=job["kwargs"],
    ))


# ── TCP ───────────────────────────────────────────────────────────────────────

async def handle_worker(reader, writer):
    worker_id = None
    labels    = []
    try:
        msg = await recv_msg(reader)
        if not isinstance(msg, Register):
            writer.close()
            return

        worker_id = msg.worker_id
        labels    = msg.labels
        _writers[worker_id] = writer

        await _save_worker(worker_id, labels, "idle")
        print(f"[coordinator] worker {worker_id} connected labels={labels}")

        await _dispatch_next()
        asyncio.create_task(_ping_loop(worker_id, writer))

        while True:
            msg = await recv_msg(reader)

            if isinstance(msg, Pong):
                pass  # heartbeat ack, nothing to do

            elif isinstance(msg, JobStatus):
                job = await _load_job(msg.job_id)
                if not job:
                    continue

                if msg.status == "running":
                    job["status"] = "running"
                    await _save_job(job)
                    await _save_worker(worker_id, labels, "busy", msg.job_id)

                elif msg.status in ("done", "error", "cancelled"):
                    job.update({
                        "status":      msg.status,
                        "result":      json.loads(msg.result) if msg.result else None,
                        "error":       msg.error,
                        "warnings":    msg.warnings,
                        "infos":       msg.infos,
                        "debug":       msg.debug,
                        "finished_at": _now(),
                    })
                    await _save_job(job)
                    await _save_worker(worker_id, labels, "idle")
                    await _dispatch_next()

            elif isinstance(msg, JobUpdate):
                # Merge meta into job
                job = await _load_job(msg.job_id)
                if job:
                    job["meta"].update(msg.meta)
                    await _save_job(job)

    except (asyncio.IncompleteReadError, ConnectionResetError):
        pass
    finally:
        if worker_id:
            _writers.pop(worker_id, None)
            await _save_worker(worker_id, labels, "offline")
            print(f"[coordinator] worker {worker_id} disconnected")


async def _ping_loop(worker_id: str, writer):
    loop = asyncio.get_event_loop()
    last_pong = loop.time()
    try:
        while _writers.get(worker_id) is writer:
            await asyncio.sleep(PING_INTERVAL)
            if _writers.get(worker_id) is not writer:
                break
            await send_msg(writer, Ping())
            await asyncio.sleep(PONG_TIMEOUT)
            # If no pong within window, drop connection
            raw = await rdb.hgetall(f"workerz:worker:{worker_id}")
            if raw.get("status") == "offline":
                break
    except Exception:
        pass


# ── HTTP ──────────────────────────────────────────────────────────────────────

async def route_post_job(request: Request):
    body = await request.json()
    labels = body.get("labels") or []

    # Fail fast if no worker with these labels is registered at all
    if labels:
        all_workers = await _load_all_workers()
        label_set   = set(labels)
        if not any(label_set.issubset(set(w["labels"])) for w in all_workers):
            return JSONResponse(
                {"error": "no_worker", "detail": f"no worker registered with labels {labels}"},
                status_code=409,
            )

    job_id = str(uuid.uuid4())
    job = {
        "id":          job_id,
        "task":        body["task"],
        "args":        body.get("args", []),
        "kwargs":      body.get("kwargs", {}),
        "labels":      labels,
        "status":      "queued",
        "worker_id":   None,
        "result":      None,
        "error":       None,
        "warnings":    [],
        "infos":       [],
        "debug":       [],
        "meta":        {},
        "created_at":  _now(),
        "finished_at": None,
    }
    await _save_job(job)

    match = await _find_idle_worker(labels)
    if match:
        wid, _ = match
        await _assign(wid, job)
    else:
        await queue.put(job)

    return JSONResponse({"job_id": job_id})


async def route_get_job(request: Request):
    job = await _load_job(request.path_params["job_id"])
    if not job:
        return JSONResponse({"error": "not found"}, status_code=404)
    return JSONResponse(job)


async def route_cancel_job(request: Request):
    job_id = request.path_params["job_id"]
    job    = await _load_job(job_id)
    if not job:
        return JSONResponse({"error": "not found"}, status_code=404)

    if job["status"] == "queued":
        job.update({"status": "cancelled", "finished_at": _now()})
        await _save_job(job)
        return JSONResponse({"cancelled": True})

    if job["status"] in ("dispatched", "running"):
        wid = job.get("worker_id")
        if wid and _writers.get(wid):
            await send_msg(_writers[wid], Cancel(job_id=job_id))
        job.update({"status": "cancelled", "finished_at": _now()})
        await _save_job(job)
        return JSONResponse({"cancelling": True})

    return JSONResponse({"error": "not cancellable", "status": job["status"]})


# ── Lifespan ──────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app):
    global rdb, queue
    queue = asyncio.Queue()
    rdb   = aioredis.from_url(REDIS_URL, decode_responses=True)

    # Re-queue jobs that were in-flight when coordinator last died
    for jid in await rdb.smembers("workerz:jobs"):
        raw = await rdb.hgetall(f"workerz:job:{jid}")
        if raw and raw.get("status") in ("queued", "dispatched"):
            job = await _load_job(jid)
            if job:
                job["status"]    = "queued"
                job["worker_id"] = None
                await _save_job(job)
                await queue.put(job)

    # Mark all workers offline (they'll re-register via TCP)
    for wid in await rdb.smembers("workerz:workers"):
        raw = await rdb.hgetall(f"workerz:worker:{wid}")
        if raw:
            await rdb.hset(f"workerz:worker:{wid}", mapping={"status": "offline", "current_job": ""})

    tcp = await asyncio.start_server(handle_worker, TCP_HOST, TCP_PORT)
    asyncio.get_event_loop().create_task(tcp.serve_forever())
    print(f"coordinator  http://0.0.0.0:{HTTP_PORT}  tcp://0.0.0.0:{TCP_PORT}  redis={REDIS_URL}")
    yield
    tcp.close()
    await rdb.aclose()


app = Starlette(
    lifespan=lifespan,
    routes=[
        Route("/job",          route_post_job,    methods=["POST"]),
        Route("/job/{job_id}", route_get_job,     methods=["GET"]),
        Route("/job/{job_id}", route_cancel_job,  methods=["DELETE"]),
    ],
)
