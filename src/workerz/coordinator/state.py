"""In-memory job/worker state. One dict each, plain getters and setters.

Lives in the coordinator process. Nothing persists: a restart loses everything.
"""


class State:
    def __init__(self):
        self._jobs:    dict[str, dict] = {}
        self._workers: dict[str, dict] = {}

    # jobs
    def get_job(self, job_id):
        return self._jobs.get(job_id)

    def set_job(self, job):
        self._jobs[job["id"]] = job

    def list_jobs(self):
        return list(self._jobs.values())

    # workers
    def get_worker(self, worker_id):
        return self._workers.get(worker_id)

    def set_worker(self, worker):
        self._workers[worker["id"]] = worker

    def list_workers(self):
        return list(self._workers.values())
