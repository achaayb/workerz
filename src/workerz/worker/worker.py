import asyncio
import importlib.util
import inspect
import json
import os
import uuid
from pathlib import Path

from workerz.protocol import Cancel, Dispatch, JobStatus, JobUpdate, Ping, Pong, Register, recv_msg, send_msg

COORDINATOR_HOST = os.environ.get("WORKERZ_COORDINATOR_HOST", "127.0.0.1")
COORDINATOR_TCP  = int(os.environ.get("WORKERZ_COORDINATOR_TCP", 7777))
LABELS           = [l.strip() for l in os.environ.get("WORKERZ_LABELS", "").split(",") if l.strip()]


class TaskContext:
    """Injected as first arg into every task. Collects logs + allows mid-run meta pushes."""

    def __init__(self, job_id: str, writer):
        self.job_id   = job_id
        self._writer  = writer
        self.warnings: list[str] = []
        self.infos:    list[str] = []
        self.debug:    list[str] = []

    def warn(self, msg: str):
        self.warnings.append(str(msg))

    def info(self, msg: str):
        self.infos.append(str(msg))

    def debug(self, msg: str):
        self.debug.append(str(msg))

    async def update(self, **meta):
        """Push arbitrary meta to coordinator immediately (visible in dashboard)."""
        await send_msg(self._writer, JobUpdate(job_id=self.job_id, meta=meta))


def load_tasks(filepath: str) -> dict:
    path   = Path(filepath).resolve()
    spec   = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return {
        name: fn
        for name, fn in inspect.getmembers(module, inspect.isfunction)
        if getattr(fn, "_is_workerz_task", False)
    }


async def run_task(fn, ctx, args, kwargs):
    if asyncio.iscoroutinefunction(fn):
        return await fn(ctx, *args, **kwargs)
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, lambda: fn(ctx, *args, **kwargs))


async def main(filepath: str):
    if not LABELS:
        raise RuntimeError("WORKERZ_LABELS is required (comma-separated list)")

    tasks     = load_tasks(filepath)
    worker_id = f"worker-{uuid.uuid4().hex[:8]}"
    active: dict[str, asyncio.Task] = {}

    print(f"[{worker_id}] labels={LABELS} tasks={list(tasks.keys())}")
    print(f"[{worker_id}] connecting {COORDINATOR_HOST}:{COORDINATOR_TCP}")

    reader, writer = await asyncio.open_connection(COORDINATOR_HOST, COORDINATOR_TCP)

    await send_msg(writer, Register(worker_id=worker_id, labels=LABELS))
    print(f"[{worker_id}] registered")

    async def execute(msg: Dispatch):
        fn  = tasks.get(msg.task)
        ctx = TaskContext(job_id=msg.job_id, writer=writer)

        if not fn:
            await send_msg(writer, JobStatus(
                job_id=msg.job_id, status="error",
                error=f"unknown task: {msg.task}",
            ))
            return

        await send_msg(writer, JobStatus(job_id=msg.job_id, status="running"))
        handle = asyncio.create_task(run_task(fn, ctx, msg.args, msg.kwargs))
        active[msg.job_id] = handle

        try:
            result = await handle
            await send_msg(writer, JobStatus(
                job_id=msg.job_id, status="done",
                result=json.dumps(result) if result is not None else None,
                warnings=ctx.warnings, infos=ctx.infos, debug=ctx.debug,
            ))

        except asyncio.CancelledError:
            await send_msg(writer, JobStatus(
                job_id=msg.job_id, status="cancelled",
                warnings=ctx.warnings, infos=ctx.infos, debug=ctx.debug,
            ))

        except Exception as e:
            await send_msg(writer, JobStatus(
                job_id=msg.job_id, status="error",
                error=str(e),
                warnings=ctx.warnings, infos=ctx.infos, debug=ctx.debug,
            ))

        finally:
            active.pop(msg.job_id, None)

    while True:
        try:
            msg = await recv_msg(reader)
        except (asyncio.IncompleteReadError, ConnectionResetError):
            print(f"[{worker_id}] coordinator disconnected, reconnecting...")
            break

        if isinstance(msg, Dispatch):
            asyncio.create_task(execute(msg))
        elif isinstance(msg, Cancel):
            handle = active.get(msg.job_id)
            if handle:
                handle.cancel()
        elif isinstance(msg, Ping):
            await send_msg(writer, Pong(worker_id=worker_id))

    writer.close()
