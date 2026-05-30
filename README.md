# workerz

Lightweight task queue. No Redis. No broker. TCP between coordinator and workers, HTTP for results.

## install

```bash
pip install workerz[coordinator]   # run the coordinator
pip install workerz[worker]        # run workers
pip install workerz[sdk]           # submit jobs from client code
pip install workerz[full]          # everything
```

## dev

```bash
pip install -e ".[full]"
```

## run

```bash
# coordinator
python3 -m workerz.coordinator
uvicorn workerz.coordinator.app:app   # same thing

# worker
python3 -m workerz.worker tasks.py
python3 -m workerz.worker tasks.py --name etl-worker
python3 -m workerz.worker tasks.py --host 10.0.0.1 --tcp-port 7777

# dashboard
open http://localhost:8000
```

## define tasks

```python
# tasks.py
from workerz.sdk.client import task

@task
def add(a, b):
    return a + b
```

## submit jobs

```python
from workerz.sdk.client import Client

c = Client()
job_id = c.run("add", args=[1, 2])
job_id = c.run("add", args=[1, 2], save=True)
job_id = c.run("add", args=[1, 2], worker="etl-worker")
c.cancel(job_id)
c.job(job_id)
c.jobs()
c.workers()
```

## env vars

| var                 | default                  | description                  |
|---------------------|--------------------------|------------------------------|
| `WORKERZ_TCP_HOST`  | `0.0.0.0`                | coordinator TCP bind host    |
| `WORKERZ_TCP_PORT`  | `7777`                   | coordinator TCP port         |
| `WORKERZ_HTTP_PORT` | `8000`                   | coordinator HTTP port        |
| `WORKERZ_DB`        | `workerz.db`             | SQLite path                  |
| `WORKERZ_RESULTS`   | `results/`               | saved result files directory |
| `WORKERZ_HOST`      | `127.0.0.1`              | worker: coordinator host     |
| `WORKERZ_HTTP`      | `http://127.0.0.1:8000`  | worker: coordinator HTTP     |
