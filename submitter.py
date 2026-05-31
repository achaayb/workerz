"""Long-running SDK consumer. Submits one of each task type per lap and
reports the previous lap's outcomes (so it never blocks on the slow task)."""
import itertools
import os
import time

from workerz.sdk.client import Client
from workerz.exceptions import NoWorkerAvailable

c = Client(os.environ.get("WORKERZ_COORDINATOR_URL", "http://127.0.0.1:8000"))

SPECS = [
    ("add",   [2, 3], {}),
    ("slow",  [],     {"seconds": 3}),
    ("fail",  [],     {}),
    ("buggy", [],     {}),
]


def report(job):
    job.wait()
    line = f"[{job.status:9}] {job.id[:8]}"
    if job.result is not None:
        line += f" | result={job.result}"
    if job.error:
        line += f" | error={job.error}"
    if job.warnings:
        line += f" | warn={job.warnings}"
    if job.infos:
        line += f" | info={job.infos}"
    if job.debug:
        line += f" | debug={job.debug}"
    print(line, flush=True)


pending = []
for _ in itertools.count():
    for j in pending:
        try:
            report(j)
        except Exception as e:
            print(f"[report-err] {j.id[:8]}: {e!r}", flush=True)
    pending = []

    for name, args, kwargs in SPECS:
        try:
            job = c.run(name, args=args, kwargs=kwargs)
            print(f"[submit   ] {name} -> {job.id[:8]}", flush=True)
            pending.append(job)
        except NoWorkerAvailable as e:
            print(f"[no-worker] {name}: {e}", flush=True)
        except Exception as e:
            print(f"[submit-err] {name}: {e!r}", flush=True)

    time.sleep(2)
