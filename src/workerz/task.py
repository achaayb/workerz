def task(fn):
    fn._is_workerz_task = True
    return fn
