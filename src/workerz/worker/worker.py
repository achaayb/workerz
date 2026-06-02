import asyncio
import importlib.util
import inspect
import json
import sys
import uuid
from pathlib import Path

from workerz.logging import setup_logging
from workerz.protocol import (
    Cancel, Dispatch, JobStatus, JobUpdate, Ping, Pong, Register,
    recv_msg, send_msg,
)
from workerz.worker.settings import settings

logger = setup_logging("worker", settings.log_file, settings.log_level)


class TaskContext:
    def __init__(self, job_id, writer):
        self.job_id = job_id
        self.writer = writer
        self.warnings = []
        self.infos = []
        self.debug_lines = []connect

    def warn(self, message):
        self.warnings.append(str(message))
        logger.warning("job {} | {}", self.job_id, message)

    def info(self, message):
        self.infos.append(str(message))
        logger.info("job {} | {}", self.job_id, message)

    def debug(self, message):
        self.debug_lines.append(str(message))
        logger.debug("job {} | {}", self.job_id, message)

    async def update(self, **meta):
        await send_msg(self.writer, JobUpdate(job_id=self.job_id, meta=meta))


def load_tasks(filepath):
    path = Path(filepath).resolve()
    spec = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return {
        name: fn
        for name, fn in inspect.getmembers(module, inspect.isfunction)
        if getattr(fn, "_is_workerz_task", False)
    }


def run_task(fn, ctx, args, kwargs):
    return fn(ctx, *args, **kwargs)


class Worker:
    def __init__(self, tasks):
        self.tasks = tasks
        self.worker_id = f"worker-{uuid.uuid4().hex[:8]}"
        self.reader = None
        self.writer = None
        self.running = {}  # job_id -> asyncio.Task

    async def connect(self):
        self.reader, self.writer = await asyncio.open_connection(
            settings.coordinator_host, settings.coordinator_tcp
        )
        await send_msg(self.writer, Register(worker_id=self.worker_id, labels=settings.labels))
        logger.info("{} registered labels={} tasks={}",
                    self.worker_id, settings.labels, list(self.tasks))

    async def serve(self):
        while True:
            message = await self.next_message()
            await self.route(message)

    async def next_message(self):
        try:
            return await recv_msg(self.reader)
        except (asyncio.IncompleteReadError, ConnectionResetError):
            self.shutdown()

    async def route(self, message):
        match message:
            case Dispatch():
                self.running[message.job_id] = asyncio.create_task(self.execute(message))
            case Cancel():
                task = self.running.get(message.job_id)
                if task:
                    task.cancel()
            case Ping():
                await send_msg(self.writer, Pong(worker_id=self.worker_id))

    async def execute(self, message):
        ctx = TaskContext(message.job_id, self.writer)
        fn = self.tasks.get(message.task)

        if fn is None:
            logger.error("job {} unknown task: {}", message.job_id, message.task)
            await self.report(ctx, "error", error=f"unknown task: {message.task}")
            return

        logger.info("job {} running task={}", message.job_id, message.task)
        await send_msg(self.writer, JobStatus(job_id=message.job_id, status="running"))

        try:
            result = await asyncio.to_thread(run_task, fn, ctx, message.args, message.kwargs)
            logger.info("job {} done", message.job_id)
            await self.report(ctx, "done", result=result)
        except asyncio.CancelledError:
            logger.warning("job {} cancelled", message.job_id)
            await self.report(ctx, "cancelled")
        except Exception as error:
            logger.opt(exception=True).error("job {} raised: {}", message.job_id, error)
            await self.report(ctx, "error", error=str(error))
        finally:
            self.running.pop(message.job_id, None)

    async def report(self, ctx, status, result=None, error=None):
        await send_msg(self.writer, JobStatus(
            job_id=ctx.job_id,
            status=status,
            result=json.dumps(result) if result is not None else None,
            error=error,
            warnings=ctx.warnings,
            infos=ctx.infos,
            debug=ctx.debug_lines,
        ))

    def shutdown(self):
        # connection lost: the worker is dead, exit so the orchestrator revives it
        logger.error("{} coordinator connection lost, exiting", self.worker_id)
        for task in self.running.values():
            task.cancel()
        if self.writer:
            self.writer.close()
        sys.exit(1)


async def main(filepath):
    worker = Worker(load_tasks(filepath))
    await worker.connect()
    await worker.serve()

def run():
    import argparse

    parser = argparse.ArgumentParser(prog="python -m workerz.worker")
    parser.add_argument("file", help="path to tasks .py file")
    args = parser.parse_args()

    if not settings.labels:
        logger.error("WORKERZ_LABELS is required (comma-separated list)")
        sys.exit(1)

    try:
        asyncio.run(main(args.file))
    except KeyboardInterrupt:
        logger.info("worker stopped")
    except SystemExit:
        raise
    except Exception:
        logger.exception("worker crashed")
        raise
