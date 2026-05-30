import time
import httpx
from workerz.task import task  # re-exported for convenience
from workerz.exceptions import JobNotFound, WorkerError, NoWorkerAvailable


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
    def __init__(self, base_url: str = "http://127.0.0.1:8000"):
        self.base = base_url.rstrip("/")

    def _get_job_raw(self, job_id: str) -> dict:
        resp = httpx.get(f"{self.base}/job/{job_id}")
        if resp.status_code == 404:
            raise JobNotFound(job_id)
        resp.raise_for_status()
        return resp.json()

    def run(self, task: str, args: list = None, kwargs: dict = None, labels: list[str] = None) -> Job:
        resp = httpx.post(f"{self.base}/job", json={
            "task": task, "args": args or [], "kwargs": kwargs or {}, "labels": labels or [],
        })
        if resp.status_code == 409:
            raise NoWorkerAvailable(resp.json().get("detail", "no worker available"))
        resp.raise_for_status()
        return Job(self._get_job_raw(resp.json()["job_id"]), self)

    def job(self, job_id: str) -> Job:
        return Job(self._get_job_raw(job_id), self)

    def cancel(self, job_id: str) -> dict:
        resp = httpx.delete(f"{self.base}/job/{job_id}")
        if resp.status_code == 404:
            raise JobNotFound(job_id)
        resp.raise_for_status()
        return resp.json()
