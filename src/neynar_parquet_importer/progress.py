"""
Globals and helpers that are used across the app. You shouldn't need to modify anything here.
"""

from multiprocessing import Value

from .logger import LOGGER


class ProgressCallback:
    """Helper class for updating the progress bars in a thread-safe way."""

    def __init__(self, progress, task_name, total_steps, enabled=True, value_type="I"):
        self.enabled = enabled
        self.progress = progress
        self.task_name = task_name
        self.task_id = progress.add_task(task_name, total=total_steps)
        self.total_steps = Value(value_type, total_steps)

    def __call__(self, advance):
        """This needs to be compatible with how boto does it's callbacks."""
        if self.enabled:
            self.progress.update(self.task_id, advance=advance)

    def more_steps(self, more_steps):
        if self.enabled:
            with self.total_steps.get_lock():
                self.total_steps.value += more_steps
                new_total = self.total_steps.value

            # TODO: prettier name on this. need the progress name and the task_name saved. the task_id is just a number
            LOGGER.debug(
                "Growing task %s (%s) total to %s",
                self.task_name,
                self.task_id,
                new_total,
            )

            self.progress.update(self.task_id, total=new_total)
