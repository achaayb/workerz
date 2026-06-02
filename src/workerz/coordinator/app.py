import asyncio
import json
import uuid
from datetime import datetime, timezone

from workerz.logging import setup_logging
from workerz.protocol import (
    Cancel, CancelJob, Dispatch, GetJob, JobStatus, JobUpdate, ListJobs,
    ListWorkers, Ping, Pong, Register, Reply, SubmitJob, recv_msg, send_msg,
)
from workerz.coordinator.settings import settings
from workerz.coordinator.state import State

logger = setup_logging("coordinator", settings.log_file, settings.log_level)


def now():
    return datetime.now(timezone.utc).isoformat()


class WorkerConn:
    def __init__(self, worker_id, labels, writer):
        self.worker_id = worker_id
        self.labels = set(labels)
        self.writer = writer
        self.status = "idle"  # idle | busy
        self.current_job = None
        self.last_pong = asyncio.get_event_loop().time()


class Coordinator:
    def __init__(self):
        self.state = State()
        self.queue = asyncio.Queue()
        self.workers = {}  # worker_id -> WorkerConn
        self.lock = asyncio.Lock()

    # ── connections ───────────────────────────────────────────────────────────

    async def handle_conn(self, reader, writer):
        try:
            first = await recv_msg(reader)
        except (asyncio.IncompleteReadError, ConnectionResetError):
            writer.close()
            return

        match first:
            case Register():
                await self.handle_worker(first, reader, writer)
            case SubmitJob() | GetJob() | CancelJob() | ListJobs() | ListWorkers():
                await self.handle_client(first, writer)
            case _:
                logger.warning("bad first frame: {}", type(first).__name__)
                writer.close()

    # ── worker channel ──────────────────────────────────────────────────────

    async def handle_worker(self, register, reader, writer):
        worker = WorkerConn(register.worker_id, register.labels, writer)
        self.workers[worker.worker_id] = worker
        self.mirror(worker)
        logger.info("worker {} connected labels={}", worker.worker_id, sorted(worker.labels))

        await self.dispatch_queued()
        heartbeat = asyncio.create_task(self.heartbeat(worker))

        try:
            while True:
                message = await recv_msg(reader)
                await self.route_worker_message(worker, message)
        except (asyncio.IncompleteReadError, ConnectionResetError):
            pass
        except Exception:
            logger.exception("error on worker {} connection", worker.worker_id)
        finally:
            heartbeat.cancel()
            self.drop_worker(worker)
            writer.close()

    async def route_worker_message(self, worker, message):
        match message:
            case Pong():
                worker.last_pong = asyncio.get_event_loop().time()
            case JobStatus():
                await self.apply_job_status(worker, message)
            case JobUpdate():
                job = self.state.get_job(message.job_id)
                if job:
                    job["meta"].update(message.meta)
                    self.state.set_job(job)

    async def apply_job_status(self, worker, message):
        job = self.state.get_job(message.job_id)
        if not job:
            return

        if message.status == "running":
            job["status"] = "running"
            self.state.set_job(job)
            return

        job.update({
            "status": message.status,
            "result": json.loads(message.result) if message.result else None,
            "error": message.error,
            "warnings": message.warnings,
            "infos": message.infos,
            "debug": message.debug,
            "finished_at": now(),
        })
        self.state.set_job(job)
        worker.status = "idle"
        worker.current_job = None
        self.mirror(worker)

        if message.status == "error":
            logger.error("job {} failed on worker {}: {}", message.job_id, worker.worker_id, message.error)
        else:
            logger.info("job {} {} on worker {}", message.job_id, message.status, worker.worker_id)
        await self.dispatch_queued()

    def drop_worker(self, worker):
        self.workers.pop(worker.worker_id, None)
        stored = self.state.get_worker(worker.worker_id)
        if stored:
            stored["status"] = "offline"
            stored["current_job"] = None
            self.state.set_worker(stored)

        # the dead worker will never report on its job, so requeue it
        if worker.current_job:
            job = self.state.get_job(worker.current_job)
            if job and job["status"] in ("dispatched", "running"):
                job["status"] = "queued"
                job["worker_id"] = None
                self.state.set_job(job)
                self.queue.put_nowait(job)
                logger.warning("requeued job {} after worker {} died", job["id"], worker.worker_id)
        logger.info("worker {} disconnected", worker.worker_id)

    async def heartbeat(self, worker):
        loop = asyncio.get_event_loop()
        try:
            while self.workers.get(worker.worker_id) is worker:
                await asyncio.sleep(settings.ping_interval)
                try:
                    await send_msg(worker.writer, Ping())
                except Exception:
                    return
                await asyncio.sleep(settings.pong_timeout)
                if loop.time() - worker.last_pong > settings.ping_interval + settings.pong_timeout:
                    logger.warning("worker {} missed heartbeat, dropping", worker.worker_id)
                    worker.writer.close()
                    return
        except asyncio.CancelledError:
            pass

    # ── dispatch ──────────────────────────────────────────────────────────────

    def find_idle_worker(self, labels):
        needed = set(labels)
        for worker in self.workers.values():
            if worker.status == "idle" and (not needed or needed.issubset(worker.labels)):
                return worker
        return None

    def has_capable_worker(self, labels):
        needed = set(labels)
        return any(not needed or needed.issubset(w.labels) for w in self.workers.values())

    async def assign(self, worker, job):
        worker.status = "busy"
        worker.current_job = job["id"]
        job["status"] = "dispatched"
        job["worker_id"] = worker.worker_id
        self.state.set_job(job)
        self.mirror(worker)
        await send_msg(worker.writer, Dispatch(
            job_id=job["id"], task=job["task"], args=job["args"], kwargs=job["kwargs"],
        ))
        logger.info("dispatched job {} task={} -> worker {}", job["id"], job["task"], worker.worker_id)

    async def enqueue(self, job):
        async with self.lock:
            worker = self.find_idle_worker(job["labels"])
            if worker:
                await self.assign(worker, job)
            else:
                self.queue.put_nowait(job)

    async def dispatch_queued(self):
        async with self.lock:
            deferred = []
            while not self.queue.empty():
                job = self.queue.get_nowait()
                worker = self.find_idle_worker(job["labels"])
                if worker:
                    await self.assign(worker, job)
                else:
                    deferred.append(job)
            for job in deferred:
                self.queue.put_nowait(job)

    def mirror(self, worker):
        self.state.set_worker({
            "id": worker.worker_id,
            "labels": sorted(worker.labels),
            "status": worker.status,
            "current_job": worker.current_job,
            "last_seen": now(),
        })

    # ── client requests ────────────────────────────────────────────────────────

    async def handle_client(self, request, writer):
        try:
            reply = await self.answer(request)
            await send_msg(writer, reply)
        except (asyncio.IncompleteReadError, ConnectionResetError):
            pass
        except Exception:
            logger.exception("error handling {}", type(request).__name__)
            await send_msg(writer, Reply(rid=request.rid, ok=False, err="internal_error"))
        finally:
            await writer.drain()
            writer.close()

    async def answer(self, request):
        match request:
            case SubmitJob():
                return await self.submit(request)
            case GetJob():
                return self.get(request)
            case CancelJob():
                return await self.cancel(request)
            case ListJobs():
                jobs = sorted(self.state.list_jobs(), key=lambda j: j["created_at"], reverse=True)
                return Reply(rid=request.rid, ok=True, data=jobs)
            case ListWorkers():
                return Reply(rid=request.rid, ok=True, data=self.state.list_workers())

    async def submit(self, request):
        labels = request.labels or []
        if labels and not self.has_capable_worker(labels):
            return Reply(rid=request.rid, ok=False, err="no_worker")

        job = {
            "id": str(uuid.uuid4()),
            "task": request.task, "args": request.args, "kwargs": request.kwargs,
            "labels": labels, "status": "queued", "worker_id": None,
            "result": None, "error": None, "warnings": [], "infos": [], "debug": [],
            "meta": {}, "created_at": now(), "finished_at": None,
        }
        self.state.set_job(job)
        await self.enqueue(job)
        logger.info("submit task={} labels={} -> {}", request.task, labels, job["id"])
        return Reply(rid=request.rid, ok=True, data={"job_id": job["id"]})

    def get(self, request):
        job = self.state.get_job(request.job_id)
        if not job:
            return Reply(rid=request.rid, ok=False, err="not_found")
        return Reply(rid=request.rid, ok=True, data=job)

    async def cancel(self, request):
        job = self.state.get_job(request.job_id)
        if not job:
            return Reply(rid=request.rid, ok=False, err="not_found")

        if job["status"] == "queued":
            job.update({"status": "cancelled", "finished_at": now()})
            self.state.set_job(job)
            logger.info("cancelled queued job {}", job["id"])
            return Reply(rid=request.rid, ok=True, data={"cancelled": True})

        if job["status"] in ("dispatched", "running"):
            worker = self.workers.get(job["worker_id"])
            if worker:
                await send_msg(worker.writer, Cancel(job_id=job["id"]))
            job.update({"status": "cancelled", "finished_at": now()})
            self.state.set_job(job)
            logger.info("cancelling running job {}", job["id"])
            return Reply(rid=request.rid, ok=True, data={"cancelling": True})

        return Reply(rid=request.rid, ok=False, err="not_cancellable")


async def main():
    coordinator = Coordinator()
    server = await asyncio.start_server(coordinator.handle_conn, settings.tcp_host, settings.tcp_port)
    logger.info("coordinator listening tcp://{}:{}  build={}  env={}",
                settings.tcp_host, settings.tcp_port, settings.build_version, settings.env)
    async with server:
        await server.serve_forever()


def run():
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("coordinator stopped")
    except Exception:
        logger.exception("coordinator crashed")
        raise
