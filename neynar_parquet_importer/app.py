"""
Globals and helpers that are used across the app. You shouldn't need to modify anything here.
"""

import logging
from threading import Lock

LOGGER = logging.getLogger("app")

# Locks for the progress bars. This allows them to update their length in a thread-safe way.
PROGRESS_BYTES_LOCK = Lock()
PROGRESS_CHUNKS_LOCK = Lock()


class ProgressCallback:
    """Helper class for updating the progress bars in a thread-safe way."""

    def __init__(self, progress, task_id, more_steps, lock):
        self.progress = progress
        self.task_id = task_id
        self.lock = lock

        if more_steps:
            self.more_steps(more_steps)

    def __call__(self, advance):
        self.progress.update(self.task_id, advance=advance)

    def more_steps(self, more_steps):
        with self.lock:
            new_total = self.progress.tasks[self.task_id].total + more_steps

            LOGGER.debug("Growing task %s total to %s", self.task_id, new_total)

            self.progress.update(self.task_id, total=new_total)
