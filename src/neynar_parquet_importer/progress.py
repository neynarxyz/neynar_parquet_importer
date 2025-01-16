"""
Globals and helpers that are used across the app. You shouldn't need to modify anything here.
"""

from multiprocessing import Value

from neynar_parquet_importer.settings import SHUTDOWN_EVENT


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

        if SHUTDOWN_EVENT.is_set():
            raise ValueError("Shutting down during progress update")

    def more_steps(self, more_steps):
        if self.enabled:
            with self.total_steps.get_lock():
                self.total_steps.value += more_steps
                new_total = self.total_steps.value

            # # TODO: this is too verbose
            # LOGGER.debug(
            #     "Growing task %s (%s) total to %s",
            #     self.task_name,
            #     self.task_id,
            #     new_total,
            # )

            # TODO: i think theres still a race condition here, but it seems to work
            self.progress.update(self.task_id, total=new_total)
