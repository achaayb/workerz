"""Long-running SDK consumer. Submits one task at a time, waits for it,
reports the outcome, then moves to the next."""
import itertools
import time

from workerz.sdk.client import Client
from workerz.exceptions import NoWorkerAvailable

c = Client()  # host/port from settings / .env

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


for _ in itertools.count():
    for name, args, kwargs in SPECS:
        try:
            job = c.run(name, args=args, kwargs=kwargs)
            print(f"[submit   ] {name} -> {job.id[:8]}", flush=True)
            report(job)
        except NoWorkerAvailable as e:
            print(f"[no-worker] {name}: {e}", flush=True)
        except Exception as e:
            print(f"[submit-err] {name}: {e!r}", flush=True)
        finally:
            time.sleep(2)
