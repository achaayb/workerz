import time

from workerz.task import task


@task
def add(ctx, a, b):
    ctx.info(f"adding {a} + {b}")
    return a + b


@task
def slow(ctx, seconds: int = 3):
    ctx.info(f"sleeping {seconds}s")
    time.sleep(seconds)
    ctx.warn("done sleeping")
    return f"slept {seconds}s"


@task
def fail(ctx):
    ctx.debug("about to fail")
    raise ValueError("intentional failure")


@task
def buggy(ctx):
    ctx.info("running faulty code")
    return 1 / 0  # unhandled ZeroDivisionError
