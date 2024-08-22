import logging
from threading import Lock

LOGGER = logging.getLogger("app")

PROGRESS_BYTES_LOCK = Lock()
PROGRESS_CHUNKS_LOCK = Lock()


class ProgressCallback:
    def __init__(self, progress, task_id, more_steps, lock):
        self.progress = progress
        self.task_id = task_id

        if more_steps:
            with lock:
                new_total = progress.tasks[task_id].total + more_steps

                LOGGER.debug("Growing task %s total to %s", task_id, new_total)

                progress.update(task_id, total=new_total)

    def __call__(self, bytes_amount):
        self.progress.update(self.task_id, advance=bytes_amount)
