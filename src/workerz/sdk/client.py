import asyncio
import time
import uuid

from workerz.protocol import (
    CancelJob, GetJob, Reply, SubmitJob, recv_msg, send_msg,
)
from workerz.task import task  # re-exported for convenience
from workerz.exceptions import JobNotFound, WorkerError, NoWorkerAvailable


async def _request(host: str, port: int, msg) -> Reply:
    reader, writer = await asyncio.open_connection(host, port)
    try:
        await send_msg(writer, msg)
        reply = await recv_msg(reader)
        if not isinstance(reply, Reply):
            raise WorkerError(f"unexpected reply: {type(reply).__name__}")
        return reply
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


def _run(coro):
    return asyncio.run(coro)


class Job:
    def __init__(self, data: dict, client: "Client"):
        self._data   = data
        self._client = client

    @property
    def id(self) -> str:
        return self._data["id"]

    @property
    def status(self) -> str:
        return self._data["status"]

    @property
    def done(self) -> bool:
        return self._data["status"] in ("done", "error", "cancelled")

    def _refresh(self):
        self._data = self._client._get_job_raw(self.id)

    def wait(self, poll: float = 0.5, timeout: float = None) -> "Job":
        start = time.monotonic()
        while not self.done:
            if timeout and time.monotonic() - start > timeout:
                raise TimeoutError(f"job {self.id} did not finish within {timeout}s")
            time.sleep(poll)
            self._refresh()
        return self

    def get(self):
        if not self.done:
            self.wait()
        return self._data.get("result")

    def get_or_raise(self):
        if not self.done:
            self.wait()
        if self._data["status"] == "error":
            raise WorkerError(self._data.get("error") or "job failed")
        return self._data.get("result")

    @property
    def result(self):        return self._data.get("result")
    @property
    def error(self):         return self._data.get("error")
    @property
    def warnings(self):      return self._data.get("warnings", [])
    @property
    def infos(self):         return self._data.get("infos", [])
    @property
    def debug(self):         return self._data.get("debug", [])
    @property
    def meta(self):          return self._data.get("meta", {})

    def __repr__(self):
        return f"<Job {self.id[:8]} status={self.status}>"


class Client:
    def __init__(self, host: str = None, port: int = None):
        from workerz.logging import setup_logging
        from workerz.sdk.settings import settings
        self.host = host or settings.coordinator_host
        self.port = int(port or settings.coordinator_tcp)
        self._log = setup_logging("sdk", settings.log_file, settings.log_level)

    async def _submit_async(self, task: str, args, kwargs, labels) -> str:
        reply = await _request(self.host, self.port, SubmitJob(
            rid=uuid.uuid4().hex, task=task,
            args=args or [], kwargs=kwargs or {}, labels=labels or [],
        ))
        if not reply.ok:
            if reply.err == "no_worker":
                self._log.warning("submit rejected: no worker for labels {}", labels)
                raise NoWorkerAvailable(f"no worker available for labels {labels}")
            self._log.error("submit failed: {}", reply.err)
            raise WorkerError(reply.err or "submit failed")
        self._log.info("submitted task={} job={}", task, reply.data["job_id"])
        return reply.data["job_id"]

    async def _get_async(self, job_id: str) -> dict:
        reply = await _request(self.host, self.port, GetJob(rid=uuid.uuid4().hex, job_id=job_id))
        if not reply.ok:
            if reply.err == "not_found":
                raise JobNotFound(job_id)
            raise WorkerError(reply.err or "get failed")
        return reply.data

    async def _cancel_async(self, job_id: str) -> dict:
        reply = await _request(self.host, self.port, CancelJob(rid=uuid.uuid4().hex, job_id=job_id))
        if not reply.ok:
            if reply.err == "not_found":
                raise JobNotFound(job_id)
            raise WorkerError(reply.err or "cancel failed")
        self._log.info("cancelled job {}", job_id)
        return reply.data

    def _get_job_raw(self, job_id: str) -> dict:
        return _run(self._get_async(job_id))

    def run(self, task: str, args: list = None, kwargs: dict = None,
            labels: list[str] = None) -> Job:
        job_id = _run(self._submit_async(task, args, kwargs, labels))
        return Job(self._get_job_raw(job_id), self)

    def job(self, job_id: str) -> Job:
        return Job(self._get_job_raw(job_id), self)

    def cancel(self, job_id: str) -> dict:
        return _run(self._cancel_async(job_id))
