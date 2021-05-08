import hashlib
import os
from typing import Optional

import luigi  # type: ignore
from luigi.contrib.opener import OpenerTarget  # type: ignore
from luigi.task import flatten  # type: ignore


class AutoRerunTask(luigi.Task):
    """
    A Luigi task that can detect when
    its requirements are out of date.

    When a pipeline is built with these tasks,
    tasks will be automatically rerun
    if their inputs or parameters are changed.
    """

    # Directory to write hash values to
    hash_dir = luigi.Parameter(significant=False)

    def content_hash(self) -> str:
        # Tasks have a hash, not their outputs
        # (as can have more than one output)
        # The hash incorporates the hashes of all its inputs
        hasher = hashlib.md5()
        for req in self._requires():
            hasher.update(req.content_hash().encode("utf-8"))

        # Then depends on the parameters for the task,
        # which are embedded in the task_id
        hasher.update(self.task_id.encode("utf-8"))

        return hasher.hexdigest()

    def hash_path(self) -> str:
        # Path to write the task hash files too,
        # which are compared for successive runs
        return os.path.join(self.hash_dir, self.task_id + ".md5")

    def complete(self) -> bool:
        # First check if any of the task's outputs are missing:
        # if so, then not complete
        for output in flatten(self.output()):
            if not output.exists():
                return False

        # Check if the old hash for the task is missing:
        # if so, then not complete
        if not os.path.exists(self.hash_path()):
            return False

        # Check if the old hash differs from the current one:
        # if so, then not complete
        old_hash: Optional[str] = None
        try:
            with open(self.hash_path(), "rt") as f:
                old_hash = f.read().strip()
        except FileNotFoundError:
            old_hash = None

        if old_hash != self.content_hash():
            return False

        # Check if any of the task's requirements are not complete:
        # if so, then not complete
        if any((not req.complete() for req in self._requires())):
            return False

        return True


@AutoRerunTask.event_handler(luigi.Event.SUCCESS)
def hash_writer(task):
    with open(task.hash_path(), "wt") as f:
        f.write(task.content_hash())


class RerunExtTask(AutoRerunTask):
    """
    Task to wrap input created externally to Luigi pipeline,
    which generates a hash of the contents,
    to use with AutoRerunTasks.
    """

    path = luigi.Parameter()

    def content_hash(self) -> str:
        # Tasks have a hash, not their outputs
        # (as can have more than one output)
        # The hash is based on the wrapped file
        hasher = hashlib.md5()

        with open(self.path, "rb") as f:
            while True:
                bytes_ = f.read(4096)
                if not bytes_:
                    break
                hasher.update(bytes_)

        return hasher.hexdigest()

    def output(self):
        return OpenerTarget(self.path)

    def run(self):
        pass


class RerunExtractOutputTask(AutoRerunTask):
    """Helper task type used for unpacking multiple outputs,
    that works with auto rerun detection."""

    output_name = luigi.Parameter()

    def output(self):
        return self.input()[self.output_name]
