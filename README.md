# What is this and why im working on it

ive been dealing with workers for a long time now, from environments that require a simple set and forget to ones that require detailed logging, chains, very long running tasks etc..

celery is fine but its awkward, if you ever actually set it up you'll know what im talking about, late ack, base memory usage, config files not registering.. celery is robust but its too limiting and abstract for what i need.

you need a broker and a result backend, you need to figure out worker modes, flower?? .. thats before youve written a single line of business logic.

workerz is different in a few ways that actually matter.

**workers pull nothing**

the coordinator pushes. the moment a job comes in and a worker is idle, it gets dispatched. no polling interval, no prefetch tuning, no `acks_late` double edged sword.

**labels instead of queues**

you dont define queues and route tasks to them. you label workers with what they can do and label jobs with what they need. the coordinator finds the match.

labels are also dynamic. in celery if you need per-user dedicated workers you either create a queue per user or have your infra change env variables before worker startup.. with workerz you just set labels at runtime. think shared hosting vs actual dedicated hosting.

```
WORKERZ_LABELS=ml,gpu  →  this worker takes ml and gpu jobs
```

**the result is always the same shape**

```json
{
  "result": ...,
  "error": null,
  "warnings": [],
  "infos": [],
  "debug": []
}
```

**structured logging**

```python
@task
def process(ctx, data):
    ctx.info("starting")
    ctx.warn("something looks off")
    result = do_work(data)
    ctx.debug(f"processed {len(result)} items")
    return result
```

goes straight onto the job record. no log aggregation setup, no grep, no sentry integration required to see what your task was doing.

**live metadata**

tasks can push updates mid-run. useful for long running jobs where you want to track progress without polling logs.

```python
@task
async def train(ctx, epochs):
    for i in range(epochs):
        loss = do_epoch(i)
        await ctx.update(epoch=i, loss=loss, progress=f"{i}/{epochs}")
```

shows up on the job immediately. no separate progress tracking, no celery custom states workaround.

---

celery is probably still better for now.. but keep an eye on this.
