class WorkerzError(Exception):
    pass

class JobNotFound(WorkerzError):
    pass

class WorkerError(WorkerzError):
    pass

class NoWorkerAvailable(WorkerzError):
    pass
